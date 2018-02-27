(ns usnpi.updater
  (:require [usnpi.db :as db]
            [clojure.tools.logging :as log]
            [dk.ative.docjure.spreadsheet :as xls]
            [clj-http.client :as client]
            [hickory.core :as hickory]
            [hickory.select :as s])
  (:import java.util.zip.ZipEntry
           java.util.zip.ZipInputStream))

(def ^:private
  path-base "http://download.cms.gov/nppes/")

(def ^:private
  path-dl "NPI_Files.html")

(defn- parse-page []
  (-> (str path-base path-dl)
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

(defn- get-deactive-url [page]
  (when-let [node (first (s/select deactive-selector page))]
    (let [href (-> node :attrs :href)]
      (full-url href))))

;; "http://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_021318.zip"

(defn- get-deactive-stream [url]
  (let [resp (client/get url {:as :stream})
        stream (ZipInputStream. (:body resp))]
    (doto stream .getNextEntry)))

(defn- read-deactive-npis [source]
  (let [wb (xls/load-workbook source)
        sheet (first (xls/sheet-seq wb))
        cell (xls/select-columns {:A :npi} sheet)
        header 2]
    (take 10 (map :npi (drop header cell)))))

(defn- by-chunks [n seq]
  (partition n n [] seq))

(defn- mark-npi-deleted [npis-all]
  (db/with-tx
    (doseq [npis (by-chunks 100 npis-all)]
      (db/execute!
       (db/to-sql
        {:update :practitioner
         :set {:deleted true}
         :where [:in :npi npis]})))))

(defn task-deactivate []
  (let [page (parse-page)
        url (get-deactive-url page)
        stream (get-deactive-stream url)
        npis (read-deactive-npis stream)]
    (mark-npi-deleted npis)
    nil))

(defn task-dissemination []
  )
