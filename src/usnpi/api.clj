(ns usnpi.api
  "REST API handlers."
  (:require [usnpi.db :as db]
            [environ.core :refer [env]]
            [cheshire.core :as json]
            [clojure.string :as str]))

(def ^:private
  env-fields
  [:GIT_COMMIT
   :FHIRTERM_BASE
   :BEAT_TIMEOUT])

(defn- turn-field
  [field]
  (-> field
      name
      str/lower-case
      (str/replace "_" "-")
      (str/replace "." "-")
      keyword))

(defn- json-resp
  [data]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/generate-string data)})

(defn api-env
  "An API that returns some of ENV vars."
  [request]
  (let [new-fields (map turn-field env-fields)
        env-values (map #(get env %) new-fields)]
    (json-resp (into {} (map vector env-fields env-values)))))

(defn api-updates
  "Returns the latest NPI updates."
  [request]
  (json-resp
   (db/query "select * from npi_updates order by date desc limit 100")))

(defn api-tasks
  "Returns the list of regular tasks."
  [request]
  (json-resp
   (db/query "select * from tasks order by id")))
