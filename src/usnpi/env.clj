(ns usnpi.env
  "Misc ENV utils and API."
  (:require [environ.core]
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

(def env environ.core/env)

(defn api-env
  "An API that returns some of ENV vars."
  [request]
  (let [new-fields (map turn-field env-fields)
        env-values (map #(get env %) new-fields)]
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/generate-string
            (into {} (map vector env-fields env-values)))}))
