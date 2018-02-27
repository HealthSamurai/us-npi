(ns usnpi.updater
  (:require [usnpi.db :as db]
            [clojure.tools.logging :as log]
            [dk.ative.docjure.spreadsheet :as xls]
            [clj-http.client :as client])
  (:import java.util.zip.ZipEntry
           java.util.zip.ZipInputStream))

(defn- dl-deact-file []


  )

;; "http://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_021318.zip"

(defn- dl-deact-file [url]
  (let [resp (client/get url {:as :stream})
        zip-stream (ZipInputStream. (:body resp))
        zip-entry (.getNextEntry zip-stream)
        wb (xls/load-workbook zip-stream)
        sheet (first (xls/sheet-seq wb))
        cell (xls/select-columns {:A :npi} sheet)

        ]

    (take 10 (map :npi (drop 2 cell)))

    )

  )


(defn- read-deact-file [filepath]

  (let [wb (xls/load-workbook filepath)
        sheet (first (xls/sheet-seq wb))
        cell (xls/select-columns {:A :npi} sheet)]
    (mapv :npi (drop 2 cell)))


  )

(defn task []


  :todo)
