(ns usnpi.core
  (:gen-class)
  (:require [usnpi.sync :as sync] 
            [usnpi.npi :as npi]
            [ring.util.codec]
            [ring.util.io]
            [org.httpkit.server :as server]
            [clojure.string :as str]
            [route-map.core :as routing]
            [cheshire.core :as json])
  (:import [java.io BufferedWriter OutputStreamWriter  ByteArrayInputStream ByteArrayOutputStream]))

(defn form-decode [s] (clojure.walk/keywordize-keys (ring.util.codec/form-decode s)))

(def routes
  {:GET (fn [req] {:status 200 :body (pr-str req)})
   "practitioner" {:GET #'npi/get-practitioners
                   [:npi] {:GET #'npi/get-practitioner}}})

(defn generate-json-stream
  ([data] (generate-json-stream data nil))
  ([data options]
   (ring.util.io/piped-input-stream
    (fn [out] (json/generate-stream
               data (-> out (OutputStreamWriter.) (BufferedWriter.)) options)))))

(defn index [{uri :uri qs :query-string :as req}]
  (if-let [h (routing/match [:get (str/lower-case uri)] routes)]
    (let [params (when qs (form-decode qs))]
      (-> ((:match h) (assoc req
                             :route-params (:params h)
                             :params params))
          (update :body (fn [x] (when x (generate-json-stream x))))
          (assoc-in [:headers "Content-Type"] "application/json")))

    {:status 404
     :body (str "Url " (str/lower-case uri) " not found " (keys routes))}))

(defn start []
  (server/run-server #'index {:port 8080}))

(defn -main [& _]
  #_(sync/init)
  (start))


(comment
  (def srv (start))

  (srv)

  )


