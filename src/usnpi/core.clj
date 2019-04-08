(ns usnpi.core
  (:gen-class)
  (:require [usnpi.npi :as npi]
            [usnpi.tasks :as tasks]
            [usnpi.beat :as beat]
            [usnpi.api :as api]
            [usnpi.db :as db]
            [usnpi.fhir :as fhir]
            [usnpi.http :as http]
            [usnpi.swagger :as swagger]
            [usnpi.search :as search]
            [usnpi.env :as env :refer [env]]
            [clojure.tools.logging :as log]
            [org.httpkit.server :as server]
            [clojure.string :as str]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.webjars :refer [wrap-webjars]]
            [route-map.core :as routing]))

(def routes-ops
  "A set of routes that cause sensible changes. For dev or non-public usage."
  {"ops" {"reset-tasks" {:GET #'api/api-reset-tasks}
          "warmup-index" {:GET #'api/api-pg-warmup-index}
          "logs" {:GET #'api/api-logs}
          "full-import" {:GET #'api/api-trigger-full-import}}})

(def routes-common
  {:GET (fn [req] {:status 200 :body (pr-str req)})

   "swagger" {:GET #'swagger/api-index
              "schema" {:GET #'swagger/api-schema}}

   "search" {:GET #'search/search}

   "practitioner" {:GET #'npi/get-practitioners
                   "$batch" {:GET #'npi/get-practitioners-by-ids}
                   [:npi] {:GET #'npi/get-practitioner}}

   "organization" {:GET #'npi/get-organizations
                   "$batch" {:GET #'npi/get-organizations-by-ids}
                   [:npi] {:GET #'npi/get-organization}}

   "metadata" {:GET #'fhir/api-metadata}

   "system" {"env" {:GET #'api/api-env}
             "updates" {:GET #'api/api-updates}
             "tasks" {:GET #'api/api-tasks}
             "beat" {:GET #'api/api-beat}
             "db" {:GET #'api/api-pg-state}}})

(def routes
  (cond-> routes-common
    (:api-ops env)
    (merge routes-ops)))

(defn- log-request
  [request]
  (log/debugf "%s %s %s"
              (-> request :request-method name .toUpperCase)
              (-> request :uri)
              (-> request :query-string (or ""))))

(defn index
  [{uri :uri qs :query-string :as req}]
  (log-request req)
  (if-let [h (routing/match [:get (str/lower-case uri)] routes)]
    ((:match h) (update req :params merge (:params h)))
    (http/http-resp 404 (format "URL %s not found." uri))))

(def app
  (-> #'index
      http/wrap-cors
      http/wrap-encoding
      wrap-keyword-params
      wrap-params
      wrap-webjars))

(defn start-server [& [{:keys [port] :as opt}]]
  (let [port (or port 8080)]
    (log/infof "Starting server on port %s..." port)
    (server/run-server app {:port port})))

(defn init [& [opt]]
  (env/init)
  (db/init)
  (tasks/init)
  (beat/init)
  (start-server opt))

(defn -main [& _]
  (init))
