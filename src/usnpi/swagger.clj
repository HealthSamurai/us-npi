(ns usnpi.swagger
  (:require [clojure.java.io :as io]
            [usnpi.http :refer [json-resp http-resp]]))

(defn api-index [request]
  (http-resp (-> "swagger/index.html" io/resource slurp)))

(defn api-schema [request]
  (json-resp (-> "swagger/swagger.edn" io/resource slurp read-string)))
