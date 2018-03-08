(ns usnpi.db
  (:require [clojure.java.jdbc :as jdbc]
            [clj-time.jdbc] ;; extends JDBC protocols
            [honeysql.core :as sql]
            [usnpi.shell :as shell]
            [clojure.tools.logging :as log]
            [migratus.core :as migratus]
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
  "Local wrapper to turn a map into a SQL string."
  [sqlmap]
  (sql/format sqlmap))

;;
;; DB API
;; Here and below: partial doesn't work with binding.
;;

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


;;
;; Custom queries
;;

(defn query-insert-practitioners
  [values]
  (let [query-map {:insert-into :practitioner
                   :values values}
        extra "ON CONFLICT (id) DO UPDATE SET deleted = EXCLUDED.deleted, resource = EXCLUDED.resource"
        query-vect (sql/format query-map)
        query-main (first query-vect)
        query-full (format "%s %s" query-main extra)]
    (into [query-full] (rest query-vect))))

;;
;; migrations
;;

(def ^:private
  mg-cfg {:store :database
          :migration-dir "migrations"
          :db *db*})

(defn- migrate []
  (log/info "Running migrations...")
  (migratus/migrate mg-cfg)
  (log/info "Migrations done."))

;;
;; Init part
;;

(defn init []
  (migrate))
