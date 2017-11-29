(ns usnpi.core
  (:gen-class)
  (:require [usnpi.sync :as sync] 
            [org.httpkit.server :as server]))

(defn index [req]
  {:status 200
   :body "Ok!"})

(defn start []
  (server/run-server #'index {:port 8080}))

(defn -main [& _]
  (sync/init))


