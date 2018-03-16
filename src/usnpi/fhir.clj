(ns usnpi.fhir
  (:require [usnpi.db :as db]
            [usnpi.http :as http]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn api-metadata
  [request]
  (let [caps (-> "FHIR/CapabilityStatement.json" io/resource slurp)]
    (http/set-json (http/http-resp caps))))
