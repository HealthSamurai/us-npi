(ns usnpi.util-test
  (:require [cheshire.core :as json]))

(defn read-json
  "Reads the data from a Ring response."
  [res]
  (json/parse-string (:body res) true))
