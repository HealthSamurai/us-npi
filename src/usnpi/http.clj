(ns usnpi.http
  "Misc HTTP utils."
  (:require [cheshire.core :as json]))

(defn set-header
  [resp h v]
  (assoc-in resp [:headers h] v))

(defn set-json
  [resp]
  (set-header resp "Content-Type" "application/json"))

(defn http-resp

  ([body]
   (http-resp 200 body))

  ([status body]
   {:status status
    :headers {}
    :body body}))

(defn json-resp

  ([data]
   (json-resp 200 data))

  ([status data]
   (let [body (json/generate-string data {:pretty true})]
     (set-json (http-resp status body)))))

(defn err-resp

  ([status msg]
   (json-resp status {:message msg}))

  ([status tpl & args]
   (err-resp status (apply format tpl args))))
