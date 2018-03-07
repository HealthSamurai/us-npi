(ns usnpi.api
  "REST API handlers."
  (:require [usnpi.db :as db]
            [usnpi.beat :as beat]
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

(defn api-beat
  "Returns the status of the beat subsystem."
  [request]
  (json-resp
   {:status (beat/status)}))

(defn api-reset-tasks
  "Resets the DB data to make all the tasks to be run immediately."
  [request]
  (db/with-tx
    (db/execute! "delete from npi_updates")
    (db/execute! "update tasks set next_run_at = (current_timestamp at time zone 'UTC') + random() * interval '600 seconds'" ))
  (when-not (beat/status)
    (beat/start))
  (json-resp
   {:status true}))
