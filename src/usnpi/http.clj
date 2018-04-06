(ns usnpi.http
  "Misc HTTP utils."
  (:require [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [clojure.pprint :as pp]
            [clojure.string :as str]))

(defn set-header
  [resp h v]
  (assoc-in resp [:headers h] v))

(defn http-resp

  ([body]
   (http-resp 200 body))

  ([status body]
   {:status status
    :headers {}
    :body body}))

(defn err-resp

  ([status msg]
   (http-resp status {:message msg}))

  ([status tpl & args]
   (err-resp status (apply format tpl args))))

(defn format-is? [request value]
  (some-> request :params :_format str/lower-case (= value)))

(defn accept-encoding? [request mime]
  (some-> request :headers (get "accept") str/lower-case (str/includes? mime)))

(defn guess-encoding
  [request]
  (cond
    (or (format-is? request "yaml")
        (accept-encoding? request "yaml"))
    :yaml

    (or (format-is? request "edn")
        (accept-encoding? request "edn"))
    :edn

    :else :json))

(defn data-response?
  [response]
  (-> response :body coll?))

(defn pprint-str [value]
  (with-out-str
    (pp/pprint value)))

(defn encode-response
  [encoding response]

  (case encoding

    :json
    (-> response
        (update :body #(json/generate-string % {:pretty true}))
        (set-header "Content-Type" "application/json"))

    :yaml
    (-> response
        (update :body yaml/generate-string)
        (set-header "Content-Type" "text/yaml"))

    :edn
    (-> response
        (update :body pprint-str)
        (set-header "Content-Type" "text/edn"))

    response))

(defn wrap-encoding
  [handler]
  (fn [request]
    (when-let [response (handler request)]
      (if (data-response? response)
        (let [encoding (guess-encoding request)]
          (encode-response encoding response))
        response))))

(defn wrap-cors
  [handler]
  (fn [{method :request-method hs :headers :as request}]
    (if (= method :options)
      {:status 200
       :body {:message "preflight complete"}
       :headers {"Access-Control-Allow-Headers" (get hs "access-control-request-headers")
                 "Access-Control-Allow-Methods" (get hs "access-control-request-method")
                 "Access-Control-Allow-Origin" (get hs "origin")
                 "Access-Control-Allow-Credentials" "true"
                 "Access-Control-Expose-Headers" "Location, Content-Location, Category, Content-Type, X-total-count"}}
      (when-let [response (handler request)]
        (update response :headers merge
                {"Access-Control-Allow-Origin" (get hs "origin")
                 "Access-Control-Allow-Credentials" "true"
                 "Access-Control-Expose-Headers" "Location, Content-Location, Category, Content-Type, X-total-count"})))))
