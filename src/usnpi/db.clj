(ns usnpi.db
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [usnpi.shell :as shell]
            [clj-time.jdbc] ;; extends SQL protocols
            [environ.core :refer [env]]))

(def ^:private
  url-template
  "jdbc:postgresql://%s:%s/%s?stringtype=unspecified&user=%s&password=%s")

(def ^:private
  db-keys [:db-host :db-port :db-database :db-user :db-password])

(def ^:private
  db-vals (mapv env db-keys))

(def ^:private
  pg-keys ["PGHOST" "PGPORT" "PGDATABASE" "PGUSER" "PGPASSWORD"])

(def ^:private
  pg-env (into {} (map vector pg-keys db-vals)))

(def ^:private
  db-url (apply format url-template db-vals))

(def ^:dynamic
  *db* {:dbtype "postgresql"
        :connection-uri db-url})

(defn psql
  "Executes a SQL file using psql utility."
  [sql-file]
  (shell/with-env pg-env
    (shell/sh "psql" "-f" sql-file)))

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
