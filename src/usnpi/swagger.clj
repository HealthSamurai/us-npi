(ns usnpi.swagger
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [usnpi.http :refer [http-resp]]))

(defn api-index [request]
  (http-resp (-> "swagger/index.html" io/resource slurp)))

(defn api-schema [request]
  (http-resp (-> "swagger/swagger.edn" io/resource slurp edn/read-string)))
