(ns usnpi.api
  "REST API handlers."
  (:require [environ.core]
            [cheshire.core :refer [env]]
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

(defn api-env
  "An API that returns some of ENV vars."
  [request]
  (let [new-fields (map turn-field env-fields)
        env-values (map #(get env %) new-fields)]
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/generate-string
            (into {} (map vector env-fields env-values)))}))

(defn api-updates
  "Returns the latest NPI updates."
  [request]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/generate-string
          (db/query "select * from npi_updates order by date desc limit 100"))})
