(ns usnpi.db
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [clj-time.jdbc]
            [environ.core :refer [env]]))

(def ^:dynamic
  *db* {:dbtype "postgresql"
        :connection-uri (env :database-url)})

(defn to-sql
  [sqlmap]
  (sql/format sqlmap))

;; Here and below: partial won't work.

(defn query [& args]
  (apply jdbc/query *db* args))

(defn get-by-id [& args]
  (apply jdbc/get-by-id *db* args))

(defn find-by-keys [& args]
  (apply jdbc/find-by-keys *db* args))

(defn insert! [& args]
  (first (apply jdbc/insert! *db* args)))

(defn insert-multi! [& args]
  (apply jdbc/insert-multi! *db* args))

(defn update! [& args]
  (apply jdbc/update! *db* args))

(defn delete! [& args]
  (apply jdbc/delete! *db* args))

(defn execute! [& args]
  (apply jdbc/execute! *db* args))

(defmacro with-tx
  "Runs a series of queries into transaction."
  [& body]
  `(jdbc/with-db-transaction [tx# *db*]
     (binding [*db* tx#]
       ~@body)))

(defmacro with-tx-test
  "The same as `with-tx` but rolls back the transaction after all."
  [& body]
  `(with-trx
     (jdbc/db-set-rollback-only! *db*)
     ~@body))
