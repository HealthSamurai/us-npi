(ns usnpi.swagger
  (:require [clojure.java.io :as io]
            [usnpi.http :refer [json-resp]]))

(defn api-index [request]
  {:status 200
   :body (-> "swagger/index.html" io/resource slurp)})

(defn api-schema [request]
  (json-resp (-> "swagger/swagger.edn" io/resource slurp read-string)))
