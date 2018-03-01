(ns usnpi.auto-update
  (:require [usnpi.db :as db]
            [usnpi.util :refer [raise!] :as util]
            [usnpi.sync :as sync]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [dk.ative.docjure.spreadsheet :as xls]
            [clj-http.client :as client]
            [hickory.core :as hickory]
            [hickory.select :as s]))

(def ^{:private true
       :doc "How many DB records to update at once."}
  db-chunk 100)

(def ^:private
  path-base "http://download.cms.gov/nppes/")

(def ^:private
  path-dl "NPI_Files.html")

(defn- parse-page
  "Returns a parsed tree for a given URL."
  [^String url]
  (-> url
      client/get
      :body
      hickory.core/parse
      hickory.core/as-hickory))

(defn- link-selector
  "Returns a selector function that is aimed to a link
  with an inner text that match a given text."
  [^String text]
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

(defn- get-deactive-url
  "Finds a URL for the last deactivation file on the download page.
  May return nil when not found."
  [page-tree]
  (when-let [node (last (s/select deactive-selector page-tree))]
    (let [href (-> node :attrs :href)]
      (full-url href))))

(defn- get-dissem-url
  "Finds a URL for the last dissemination file on the download page.
  May return nil when not found."
  [page-tree]
  (when-let [node (last (s/select dissem-selector page-tree))]
    (let [href (-> node :attrs :href)]
      (full-url href))))

(defn- read-deactive-npis
  "Returns a vector of NPI string IDs for a give Excel file (or a stream)."
  [source]
  (let [wb (xls/load-workbook source)
        sheet (first (xls/sheet-seq wb))
        cell (xls/select-columns {:A :npi} sheet)
        header 2]
    (mapv :npi (drop header cell))))

(defn- by-chunks [n seq]
  (partition n n [] seq))

(defn- mark-npi-deleted
  "Marks practitioners as deleted by passed NPIs."
  [npis-all]
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

(defn- file-name
  [^java.io.File file]
  (.getName file))

(defn- join-paths
  [path1 path2 & more]
  (str/join java.io.File/separator
            (into [path1 path2] more)))

(defn task-deactivate
  "A regular task that parses the download page, fetches an Excel file
  and marks the corresponding DB records as deleted."
  []
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

(defn task-dissemination
  "A regular task that parses the download page, fetches a CSV file
  and inserts/updates the existing practitioners."
  []
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

          path-rel (join-paths folder (-> path-csv io/file file-name))
          table-name (format "temp_%s" ts)
          sql (sync/sql-dissem {:path-csv path-rel
                                :path-import path-csv
                                :table-name table-name})

          sql-name "dissemination.sql"
          sql-path (join-paths folder sql-name)]

      (util/in-dir folder
        (util/spit* sql-name sql))
      (log/infof "Dissemination SQL is %s" sql-path)

      (log/infof "Running dissemination SQL againts the DB")
      (db/execute! sql)))

  nil)
