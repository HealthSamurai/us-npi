(ns usnpi.util-test
  (:require [cheshire.core :as json]))

(defn read-body
  "Reads the data from a Ring response."
  [res]
  (-> res :body (json/parse-string true)))
