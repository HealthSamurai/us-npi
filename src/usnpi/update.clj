(ns usnpi.update
  (:require [usnpi.db :as db]
            [usnpi.error :refer [error!]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [cheshire.core :as json]
            [dk.ative.docjure.spreadsheet :as xls]
            [clj-http.client :as client]
            [usnpi.models :as models]
            [hickory.core :as hickory]
            [hickory.select :as s])
  (:import java.util.zip.ZipEntry
           java.util.zip.ZipInputStream))

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

(defn- pract->db-row
  [practitioner]
  {:id (:id practitioner)
   :resource (json/generate-string practitioner)
   :deleted false})

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
;; streams
;;


(defn- ^ZipInputStream get-stream
  [url]
  (let [resp (client/get url {:as :stream})]
    (ZipInputStream. (:body resp))))

(defn- seek-stream
  [^ZipInputStream stream re]
  (loop []
    (if-let [^ZipEntry entry (.getNextEntry stream)]
      (let [filename (.getName entry)]
        (if (re-find re filename)
          true
          (recur))))))

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

      (let [stream (get-stream url-zip)
            result (seek-stream stream re-any-xlsx)]

        (when-not result
          (error! "Cannot find a deactivation file an archive."))

        (log/infof "Reading NPIs from a stream")
        (let [npis (read-deactive-npis stream)]

          (log/infof "Found %s NPIs to deactive" (count npis))

          (log/infof "Marking NPIs as deleted with a step of %s" db-chunk)
          (mark-npi-deleted npis))

        (log/infof "Saving update to the DB with URL %s" url-zip)
        (save-deactivation url-zip))))
  nil)

(defn- process-dissemination
  [stream]
  (let [step 1000
        practitioners (models/read-practitioners stream)]
    (db/with-tx
      (doseq [chunk (by-chunks step practitioners)]
        (let [rows (map pract->db-row chunk)]
          (log/infof "Inserting %s dissemination rows..." step)
          (db/execute! (db/query-insert-practitioners rows)))))))

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

      (let [stream (get-stream url-zip)
            result (seek-stream stream re-dissem-csv)]

        (when-not result
          (error! "Cannot find a dissemination file an archive."))

        (process-dissemination stream)

        (log/infof "Saving DB dissemination for the URL %s" url-zip)
        (save-dissemination url-zip))))
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

      (let [stream (get-stream url-zip)
            result (seek-stream stream re-dissem-full-csv)]

        (when-not result
          (error! "Cannot find a FULL dissemination file an archive."))

        (process-dissemination stream)

        (log/infof "Saving DB FULL dissemination for the URL %s" url-zip)
        (save-dissemination-full url-zip))))
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
