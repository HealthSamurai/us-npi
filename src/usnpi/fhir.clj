(ns usnpi.fhir
  (:require [usnpi.db :as db]
            [usnpi.http :as http]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn api-metadata
  [request]
  (let [caps (-> "FHIR/CapabilityStatement.json" io/resource slurp (json/parse-string true))]
    (http/http-resp caps)))
