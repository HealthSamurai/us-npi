(ns usnpi.db
  (:require [clojure.java.jdbc :as jdbc]
            [environ.core :as env]))

(def ^:private
  ;; db-url "jdbc:postgresql://localhost:5678/usnpi?stringtype=unspecified&user=postgres&password=verysecret"
  db-url "jdbc:postgresql://localhost:5432/usnpi?stringtype=unspecified&user=usnpi&password=usnpi"
  )

(def ^:private ^:dynamic
  *db* {:dbtype "postgresql"
        :connection-uri (or (env/env :database-url) db-url)})

(defn query [& args]
  (apply jdbc/query *db* args))

(defn get-by-id [& args]
  (apply jdbc/get-by-id *db* args))

(defn find-by-keys [& args]
  (apply jdbc/find-by-keys *db* args))

(defn insert! [& args]
  (apply jdbc/insert! *db* args))

(defn update! [& args]
  (apply jdbc/update! *db* args))

(defn delete! [& args]
  (apply jdbc/delete! *db* args))

(defn execute! [& args]
  (apply jdbc/execute! *db* args))

(defmacro with-trx [& body]
  `(jdbc/with-db-transaction [tx# *db*]
     (binding [*db* tx#]
       ~@body)))

(defmacro with-trx-test [& body]
  `(with-trx
     (jdbc/db-set-rollback-only! *db*)
     ~@body))
