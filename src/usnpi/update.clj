(ns usnpi.update
  (:require [usnpi.db :as db]
            [usnpi.time :as time]
            [usnpi.error :refer [error!]]
            [usnpi.util :as util]
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
  url-base "http://download.cms.gov/nppes")

(def ^:private
  url-download (str url-base "/NPI_Files.html"))

(defn- parse-page
  "Returns a parsed tree for a given URL."
  [^String url]
  (-> url
      client/get
      :body
      hickory.core/parse
      hickory.core/as-hickory))

(def ^:private
  parse-dl-page
  (partial parse-page url-download))

(defn- full-url [href]
  (let [href (if (= (first href) \.)
               (subs href 1)
               href)]
    (str url-base href)))

(defn- url->name [url]
  (last (str/split url #"/")))

(defn- parse-download-url
  "For a given regex and a parsed page, tries to find the latest
  <a> node which inner text matches the regex. Than takes its href value
  and composes the full URL to download a file.
  Returns nil when no nodes were found."
  [regex page-tree]
  (let [selector (s/and (s/tag :a) (s/find-in-text regex))]
    (when-let [node (last (s/select selector page-tree))]
      (let [href (-> node :attrs :href)]
        (full-url href)))))

(def ^:private
  parse-deact-url
  (partial parse-download-url
           #"NPPES Data Dissemination - Monthly Deactivation Update"))

(def ^:private
  parse-dissem-url
  (partial parse-download-url
           #"NPPES Data Dissemination - Weekly Update"))

(def ^:private
  parse-dissem-full-url
  (partial parse-download-url
           #"NPPES Data Dissemination \("))

(defn- read-deactive-npis
  "Returns a vector of NPI string IDs for a given Excel file (or a stream)."
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

(def ^:private
  re-any-xlsx #"(?i)\.xlsx$")

(def ^:private
  re-dissem-csv #"(?i)npidata_pfile.+?\.csv$")

(def ^:private
  re-dissem-full-csv #"(?i)npidata_\d+?-\d+?\.csv$")

(defn- file-name
  [^java.io.File file]
  (.getName file))

(defn- join-paths
  [path1 path2 & more]
  (str/join java.io.File/separator
            (into [path1 path2] more)))

(defn task-deactivation
  "A regular task that parses the download page, fetches an Excel file
  and marks the corresponding DB records as deleted."
  []
  (let [_ (log/info "Parsing download page...")
        page-tree (parse-dl-page)

        url-zip (parse-deact-url page-tree)
        _ (log/infof "Deactivation URL is %s" url-zip)

        _ (when-not url-zip
            (error! "Deactivation URL is missing"))

        ts (time/epoch)
        folder (format "%s-Deactivation" ts)
        zipname (url->name url-zip)]

    (util/in-dir folder
      (log/infof "Downloading file %s" url-zip)
      (util/curl url-zip zipname)
      (log/infof "Unzipping file %s" zipname)
      (util/unzip zipname))

    (let [xls-path (util/find-file folder re-any-xlsx)

          _ (when-not xls-path
              (error! "No Excel file found in %s" folder))

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
  (let [_ (log/info "Parsing download page...")
        page-tree (parse-dl-page)

        url-zip (parse-dissem-url page-tree)
        _ (log/infof "Dissemination URL is %s" url-zip)

        _ (when-not url-zip
            (error! "Dissemination URL is missing"))

        ts (time/epoch)
        folder (format "%s-Dissemination" ts)
        zipname (url->name url-zip)]

    (util/in-dir folder
      (log/infof "Downloading file %s" url-zip)
      (util/curl url-zip zipname)
      (log/infof "Unzipping file %s" zipname)
      (util/unzip zipname))

    (let [path-csv (util/find-file folder re-dissem-csv)

          _ (when-not path-csv
              (error! "No CSV Dissemination file found in %s" folder))

          path-rel (join-paths folder (-> path-csv io/file file-name))
          table-name (format "temp_%s" ts)
          sql (sync/sql-dissem {:path-csv path-rel
                                :path-import path-csv
                                :table-name table-name})

          sql-path (join-paths folder "dissemination.sql")]

      (util/spit* sql-path sql)
      (log/infof "Dissemination SQL is %s" sql-path)

      (log/infof "Running dissemination SQL from %s" sql-path)
      (db/execute! sql)
      (log/info "SQL done.")))

  nil)

(defn- practitioner-exists?
  []
  (boolean
   (not-empty
    (db/query "select id from practitioner limit 1"))))

(defn- task-full-dissemination-inner
  "See task-full-dissemination docstring."
  []
  (let [_ (log/info "Parsing download page...")
        page-tree (parse-dl-page)

        url-zip (parse-dissem-full-url page-tree)
        _ (log/infof "FULL dissemination URL is %s" url-zip)

        _ (when-not url-zip
            (error! "FULL dissemination URL is missing"))

        ts (time/epoch)
        folder (format "%s-Full-dissemination" ts)
        zipname (url->name url-zip)]

    (util/in-dir folder
      (log/infof "Downloading file %s" url-zip)
      (util/curl url-zip zipname)
      (log/infof "Unzipping file %s" zipname)
      (util/un7z zipname)) ;; unzip fails on over-4Gb files

    (let [path-csv (util/find-file folder re-dissem-full-csv)

          _ (when-not path-csv
              (error! "No FULL CSV Dissemination file found in %s" folder))

          path-rel (join-paths folder (-> path-csv io/file file-name))
          table-name (format "temp_%s" ts)
          sql (sync/sql-dissem {:path-csv path-rel
                                :path-import path-csv
                                :table-name table-name})

          sql-path (join-paths folder "dissemination-full.sql")]

      (util/spit* sql-path sql)
      (log/infof "FULL Dissemination SQL is %s" sql-path)

      (log/infof "Running FULL dissemination SQL from %s" sql-path)
      (db/execute! sql)
      (log/info "SQL done."))

    nil))

(defn task-full-disseminationt
  "Performs a FULL dissemination data import.
  Since a data file exceeds 4GB, the process might take quite long.
  Checks whether there are any practitioner records in the DB.
  Does nothing when there are."
  []
  (if (practitioner-exists?)
    (log/info "Practitioner records were found. Skipping FULL import.")
    (do
      (log/info "No practitioner records were found. Start FULL import.")
      (task-full-dissemination-inner))))
