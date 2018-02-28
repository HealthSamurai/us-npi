(ns usnpi.updater
  (:require [usnpi.db :as db]
            [usnpi.util :refer [raise!]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [dk.ative.docjure.spreadsheet :as xls]
            [clj-http.client :as client]
            [hickory.core :as hickory]
            [hickory.select :as s])
  (:import java.util.zip.ZipEntry
           java.util.zip.ZipInputStream))

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

(defn- ^ZipInputStream get-stream
  [url]
  (let [resp (client/get url {:as :stream})]
    (ZipInputStream. (:body resp))))

(defn- matches-ext [filename ext]
  (str/ends-with? (str/lower-case filename) (str/lower-case ext)))

(defn- seek-stream
  [^ZipInputStream stream ^String ext]
  (loop []
    (if-let [entry (.getNextEntry stream)]
      (let [filename (.getName entry)]
        (if (matches-ext filename ext)
          stream
          (recur)))
      (raise! "Cannot find a %s file in a stream" ext))))

(defn- seek-deactive-stream
  [^ZipInputStream stream]
  (.getNextEntry stream))

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

(defn task-deactivate []
  (let [url-page (str path-base path-dl)

        _ (log/infof "Parsing %s page..." url-page)
        page-tree (parse-page url-page)

        url-zip (get-deactive-url page-tree)
        _ (log/infof "Deactivation URL is %s" url-zip)

        _ (when-not url-zip
            (raise! "Deactivation URL doesn't present on the page %s" url-page))

        stream (get-stream url-zip)

        _ (log/infof "Seeking stream for an Excel file...")
        stream (seek-stream stream ".xlsx")

        _ (log/infof "Reading NPIs from a file...")
        npis (read-deactive-npis stream)
        _ (log/infof "Found %s NPIs to deactive" (count npis))

        ]

    (log/infof "Marking NPIs as deleted with a step of %s" db-chunk)
    (mark-npi-deleted npis)
    (log/infof "Done.")

    nil))

(defn task-dissemination []
  )
