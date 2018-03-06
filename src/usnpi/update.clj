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

;; npidata_pfile_20180219-20180225.csv
(def ^:private
  re-dissem-csv #"(?i)npidata_pfile_\d{8}-\d{8}\.csv$")

;; npidata_20050523-20180213.csv
(def ^:private
  re-dissem-full-csv #"(?i)npidata_\d{8}-\d{8}\.csv$")

(defn- join-path
  [path1 path2 & more]
  (str/join java.io.File/separator
            (into [path1 path2] more)))

(defn- file-near
  [origin another]
  (let [file (io/file origin)
        path (.getParent file)]
    (.getPath (io/file path another))))

(defn- heal-csv
  "Cuts down empty pairs of double quotes and dummy statements from a CSV file."
  [scv-input csv-output]
  (util/exec-in-current-dir!
   (format "cat %s | sed  -e 's/,\"\"/,/g ; s/\"<UNAVAIL>\"//g' > %s"
           scv-input csv-output)))

;;
;; updates
;;

(def ^:private type-deactivation "deactivation")
(def ^:private type-dissemination "dissemination")
(def ^:private type-dissemination-full "dissemination-full")

(defn- find-update
  [type url]
  (first (db/find-by-keys :npi_updates {:url url :type type})))

(defn- save-update
  [type url]
  (db/insert! :npi_updates {:url url :type type}))

(def ^:private
  find-deactivation
  (partial find-update type-deactivation))

(def ^:private
  find-dissemination
  (partial find-update type-dissemination))

(def ^:private
  find-dissemination-full
  (partial find-update type-dissemination-full))

(def ^:private
  save-deactivation
  (partial save-update type-deactivation))

(def ^:private
  save-dissemination
  (partial save-update type-dissemination))

(def ^:private
  save-dissemination-full
  (partial save-update type-dissemination-full))

;;
;; tasks
;;

(defn task-deactivation
  "A regular task that parses the download page, fetches an Excel file
  and marks the corresponding DB records as deleted."
  []
  (log/info "Parsing download page...")
  (let [page-tree (parse-dl-page)
        url-zip (parse-deact-url page-tree)]

    (if url-zip
      (log/infof "Deactivation URL is %s" url-zip)
      (error! "Deactivation URL is missing"))

    (if-let [upd (find-deactivation url-zip)]
      (log/infof "The deactivation URL %s has already been loaded." url-zip)

      (let [ts (time/epoch)
            folder (format "%s-Deactivation" ts)
            zipname (url->name url-zip)]

        (util/in-dir folder
          (log/infof "Downloading file %s" url-zip)
          (util/curl url-zip zipname)
          (log/infof "Unzipping file %s" zipname)
          (util/unzip zipname))

        (if-let [xls-path (util/find-file folder re-any-xlsx)]

          (let [_ (log/infof "Reading NPIs from %s" xls-path)
                npis (read-deactive-npis xls-path)]

            (log/infof "Found %s NPIs to deactive" (count npis))

            (log/infof "Marking NPIs as deleted with a step of %s" db-chunk)
            (mark-npi-deleted npis)

            (log/infof "Saving update to the DB with URL %s" url-zip)
            (save-deactivation url-zip)

            (log/infof "Deleting dir %s" folder)
            (util/rm-rf folder))

          (error! "No Excel file found in %s" folder)))))
  nil)

(defn task-dissemination
  "A regular task that parses the download page, fetches a CSV file
  and inserts/updates the existing practitioners."
  []
  (log/info "Parsing download page...")
  (let [page-tree (parse-dl-page)
        url-zip (parse-dissem-url page-tree)]

    (if url-zip
      (log/infof "Dissemination URL is %s" url-zip)
      (error! "Dissemination URL is missing"))

    (if-let [upd (find-dissemination url-zip)]
      (log/infof "The dissemination URL %s has already been loaded." url-zip)

      (let [ts (time/epoch)
            folder (format "%s-Dissemination" ts)
            zipname (url->name url-zip)]

        (util/in-dir folder
          (log/infof "Downloading file %s" url-zip)
          (util/curl url-zip zipname)
          (log/infof "Unzipping file %s" zipname)
          (util/unzip zipname))

        (if-let [csv-full-path (util/find-file folder re-dissem-csv)]

          (let [fix-filename "data.csv"
                csv-fix-name (file-near csv-full-path fix-filename)
                csv-rel-name (join-path folder fix-filename)
                table-name (format "temp_%s" ts)
                sql-params {:path-csv csv-rel-name
                            :path-import csv-fix-name
                            :table-name table-name}
                sql-full-path (file-near csv-full-path "dissemination.sql")]

            (log/infof "Healing CSV: %s to %s" csv-full-path csv-fix-name)
            (heal-csv csv-full-path csv-fix-name)

            (let [sql (sync/sql-dissem sql-params)]

              (log/infof "Saving dissemination SQL into %s" sql-full-path)
              (spit sql-full-path sql)

              (log/infof "Running dissemination SQL from %s" sql-full-path)
              (db/psql sql-full-path)
              (log/info "SQL done."))

            (log/infof "Saving DB dissemination for the URL %s" url-zip)
            (save-dissemination url-zip)

            (log/infof "Deleting dir %s" folder)
            (util/rm-rf folder))

          (error! "No CSV Dissemination file found in %s" folder)))))
  nil)

(defn- practitioner-exists?
  []
  (boolean
   (not-empty
    (db/query "select id from practitioner limit 1"))))

(defn- task-full-dissemination-inner
  "See task-full-dissemination docstring."
  []
  (log/info "Parsing download page...")
  (let [page-tree (parse-dl-page)
        url-zip (parse-dissem-full-url page-tree)]

    (if url-zip
      (log/infof "FULL dissemination URL is %s" url-zip)
      (error! "FULL dissemination URL is missing"))

    (if-let [upd (find-dissemination-full url-zip)]
      (log/infof "The FULL dissemination URL %s has already been loaded." url-zip)

      (let [ts (time/epoch)
            folder (format "%s-Full-dissemination" ts)
            zipname (url->name url-zip)]

        (util/in-dir folder
          (log/infof "Downloading file %s" url-zip)
          (util/curl url-zip zipname)
          (log/infof "Unzipping file %s" zipname)
          ;; 7z but not unzip since the latest fails on over-4Gb files
          (util/un7z zipname))

        (if-let [csv-full-path (util/find-file folder re-dissem-full-csv)]

          (let [fix-filename "data.csv"
                csv-fix-name (file-near csv-full-path fix-filename)
                csv-rel-name (join-path folder fix-filename)
                table-name (format "temp_%s" ts)
                sql-params {:path-csv csv-rel-name
                            :path-import csv-fix-name
                            :table-name table-name}
                sql-full-path (file-near csv-full-path "dissemination-full.sql")]

            (log/infof "Healing CSV: %s to %s" csv-full-path csv-fix-name)
            (heal-csv csv-full-path csv-fix-name)

            (let [sql (sync/sql-dissem sql-params)]

              (log/infof "Saving FULL Dissemination SQL into %s" sql-full-path)
              (spit sql-full-path sql)

              (log/infof "Running FULL dissemination SQL from %s" sql-full-path)
              (db/psql sql-full-path)
              (log/info "SQL done."))

            (log/infof "Saving DB FULL dissemination for the URL %s" url-zip)
            (save-dissemination-full url-zip)

            (log/infof "Deleting dir %s" folder)
            (util/rm-rf folder))

          (error! "No FULL CSV Dissemination file found in %s" folder)))))
  nil)

(defn task-full-dissemination
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
