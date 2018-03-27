(ns usnpi.update
  (:require [usnpi.db :as db]
            [usnpi.error :refer [error!]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [dk.ative.docjure.spreadsheet :as xls]
            [clj-http.client :as client]
            [usnpi.models :as models]
            [hickory.core :as hickory]
            [hickory.select :as s])

  ;; better support for zip-streams
  ;; https://stackoverflow.com/a/15522678/1376325
  (:import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
           org.apache.commons.compress.archivers.ArchiveEntry))

(def ^{:private true
       :doc "How many DB records to update at once."}
  db-chunk 1000)

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
  "Marks models as deleted by passed NPIs."
  [npis-all]
  (doseq [npis (by-chunks db-chunk npis-all)]
    (db/execute! (db/query-delete-practitioners npis))
    (db/execute! (db/query-delete-organizations npis))))

(def ^:private
  re-any-xlsx #"(?i)\.xlsx$")

;; npidata_pfile_20180219-20180225.csv
(def ^:private
  re-dissem-csv #"(?i)_\d{8}-\d{8}\.csv$")

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

(defn- ^ZipArchiveInputStream get-stream
  "Returns a zip stream for a URL."
  [url]
  (let [resp (client/get url {:as :stream})]
    (-> resp :body ZipArchiveInputStream.)))

(defn- seek-stream
  "For a given zip stream, tries to position its internal pointer
  to a specific file entry matching its name against a given pattern.
  Returns true or false meaning whether it was successful or not."
  [^ZipArchiveInputStream stream re]
  (loop []
    (if-let [^ArchiveEntry entry (.getNextEntry stream)]
      (let [filename (.getName entry)]
        (log/infof "Zip entry %s found in stream" filename)
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
          (error! "Cannot find a deactivation file in archive."))

        (log/infof "Reading NPIs from a stream")
        (let [npis (read-deactive-npis stream)]

          (log/infof "Found %s NPIs to deactive" (count npis))

          (log/infof "Marking NPIs as deleted with a step of %s" db-chunk)
          (mark-npi-deleted npis))

        (log/infof "Saving deactivation update with URL %s" url-zip)
        (save-deactivation url-zip))))
  nil)

(defn- process-dissemination
  "Reads models from a stream and updates them in the DB."
  [stream]
  (let [model-seq (models/read-models stream)]
    (doseq [models (by-chunks db-chunk model-seq)]
      (let [practitioners (filter models/practitioner? models)
            organizations (filter models/organization? models)]

        (when-let [rows (not-empty (map db/model->row practitioners))]
          (db/execute! (db/query-insert-practitioners rows)))

        (when-let [rows (not-empty (map db/model->row organizations))]
          (db/execute! (db/query-insert-organizations rows)))))))

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
          (error! "Cannot find a dissemination file in archive."))

        (process-dissemination stream)

        (log/infof "Saving DB dissemination update with URL %s" url-zip)
        (save-dissemination url-zip))))
  nil)

(defn- npi-exists?
  "Checks if there is something loaded in the DB."
  []
  (boolean
   (not-empty
    (db/query "select id from practitioner union select id from organizations limit 1"))))

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
            result (seek-stream stream re-dissem-csv)]

        (when-not result
          (error! "Cannot find a FULL dissemination file in archive."))

        (process-dissemination stream)

        (log/infof "Saving DB FULL dissemination update with URL %s" url-zip)
        (save-dissemination-full url-zip))))
  nil)

(defn task-full-dissemination
  "Performs a FULL dissemination data import.
  Since a data file exceeds 4GB, the process might take quite long.
  Checks whether there are any practitioner records in the DB.
  Does nothing when there are."
  []
  (if (npi-exists?)
    (log/info "NPI records were found. Skipping FULL import.")
    (do
      (log/info "No practitioner records were found. Start FULL import.")
      (task-full-dissemination-inner))))
