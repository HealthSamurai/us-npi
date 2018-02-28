(ns usnpi.updater
  (:require [usnpi.db :as db]
            [usnpi.util :refer [raise!] :as util]
            [usnpi.sync :as sync]
            [clojure.java.io :as io] ;; todo
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [dk.ative.docjure.spreadsheet :as xls]
            [clj-http.client :as client]
            [hickory.core :as hickory]
            [hickory.select :as s]))

(def ^:private
  db-chunk 100)

(def ^:private
  path-base "http://download.cms.gov/nppes/")

(def ^:private
  path-dl "NPI_Files.html")

(defn- parse-page [url]
  (-> url
      client/get
      :body
      hickory.core/parse
      hickory.core/as-hickory))

(defn- link-selector
  [text]
  (s/and
   (s/tag :a)
   (s/find-in-text (re-pattern text))))

(def ^:private
  deactive-selector
  (link-selector "NPPES Data Dissemination - Monthly Deactivation Update"))

(def ^:private
  dissem-selector
  (link-selector "NPPES Data Dissemination - Weekly Update"))

(defn- full-url [href]
  (str path-base href))

(defn- get-deactive-url [page-tree]
  (when-let [node (last (s/select deactive-selector page-tree))]
    (let [href (-> node :attrs :href)]
      (full-url href))))

(defn- get-dissem-url [page-tree]
  (when-let [node (last (s/select dissem-selector page-tree))]
    (let [href (-> node :attrs :href)]
      (full-url href))))

(defn- read-deactive-npis [source]
  (let [wb (xls/load-workbook source)
        sheet (first (xls/sheet-seq wb))
        cell (xls/select-columns {:A :npi} sheet)
        header 2]
    (mapv :npi (drop header cell))))

(defn- by-chunks [n seq]
  (partition n n [] seq))

(defn- mark-npi-deleted [npis-all]
  (db/with-tx
    (doseq [npis (by-chunks db-chunk npis-all)]
      (db/execute!
       (db/to-sql
        {:update :practitioner
         :set {:deleted true}
         :where [:in :id npis]})))))

(defn- url->name [url]
  (last (str/split url #"/")))

(def ^:private
  re-any-xlsx #"(?i)\.xlsx$")

(def ^:private
  re-dissem-csv #"(?i)npidata_pfile.+?\.csv$")

(defn task-deactivate []
  (let [url-page (str path-base path-dl)

        _ (log/infof "Parsing %s page..." url-page)
        page-tree (parse-page url-page)

        url-zip (get-deactive-url page-tree)
        _ (log/infof "Deactivation URL is %s" url-zip)

        _ (when-not url-zip
            (raise! "Deactivation URL is missing"))

        folder (format "%s-Deactivation" (util/epoch))
        zipname (url->name url-zip)]

    (util/in-dir folder
      (util/curl url-zip zipname)
      (util/unzip zipname))

    (let [xls-path (util/find-file folder re-any-xlsx)

          _ (when-not xls-path
              (raise! "No Excel file found in %s" zipname))

          _ (log/infof "Reading NPIs from %s" xls-path)
          npis (read-deactive-npis xls-path)
          _ (log/infof "Found %s NPIs to deactive" (count npis))]

      (log/infof "Marking NPIs as deleted with a step of %s" db-chunk)
      (mark-npi-deleted npis)
      (log/infof "Done."))

    nil))

(defn task-dissemination []
  (let [url-page (str path-base path-dl)

        _ (log/infof "Parsing %s page..." url-page)
        page-tree (parse-page url-page)

        url-zip (get-dissem-url page-tree)
        _ (log/infof "Dissemination URL is %s" url-zip)

        _ (when-not url-zip
            (raise! "Dissemination URL is missing"))

        ts (util/epoch)
        folder (format "%s-Dissemination" ts)
        zipname (url->name url-zip)]

    (util/in-dir folder
      (util/curl url-zip zipname)
      (util/unzip zipname))

    (let [path-csv (util/find-file folder re-dissem-csv)

          _ (when-not path-csv
              (raise! "No CSV Dissemination file found in %s" zipname))

          path-rel (str folder "/" (-> path-csv java.io.File. .getName))
          table-name (format "temp_%s" ts)

          sql (sync/sql-dissem {:path-csv path-rel
                                :path-import path-csv
                                :table-name table-name})

          sql-name "dissemination.sql"
          sql-path (str folder "/" sql-name)]

      (util/in-dir folder
        (util/spit* sql-name sql))
      (log/infof "Dissemination SQL is %s" sql-path)

      (log/infof "Running dissemination SQL againts the DB")
      (db/execute! sql)))

  nil)
