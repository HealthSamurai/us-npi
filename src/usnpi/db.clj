(ns usnpi.db
  (:require [clojure.java.jdbc :as jdbc]
            [clj-time.jdbc] ;; extends JDBC protocols
            [honeysql.core :as sql]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [migratus.core :as migratus]
            [usnpi.env :refer [env]]))


(def ^:private
  db-url
  (format "jdbc:postgresql://%s:%s/%s?stringtype=unspecified&user=%s&password=%s"
          (:db-host env)
          (:db-port env)
          (:db-database env)
          (:db-user env)
          (:db-password env)))

(def ^:dynamic
  *db* {:dbtype "postgresql"
        :connection-uri db-url})

;;
;; Helpers
;;

(defn to-sql
  "Local wrapper to turn a map into a SQL string."
  [sqlmap]
  (sql/format sqlmap))

(def raw sql/raw)

(defn model->row
  "Turns a model map into its database representation."
  [model]
  {:id (:id model)
   :resource (json/generate-string model)
   :deleted false})

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
  `(with-tx
     (jdbc/db-set-rollback-only! *db*)
     ~@body))

;;
;; Custom queries
;;

(defn- query-insert-models
  [table rows]
  (let [query-map {:insert-into table
                   :values rows}
        extra "ON CONFLICT (id) DO UPDATE SET deleted = EXCLUDED.deleted, resource = EXCLUDED.resource"
        query-vect (sql/format query-map)
        query-main (first query-vect)
        query-full (format "%s %s" query-main extra)]
    (into [query-full] (rest query-vect))))

(def query-insert-practitioners
  (partial query-insert-models :practitioner))

(def query-insert-organizations
  (partial query-insert-models :organizations))

(defn- query-delete-models
  [table npis]
  (to-sql {:update table
           :set {:deleted true}
           :where [:in :id npis]}))

(def query-delete-practitioners
  (partial query-delete-models :practitioner))

(def query-delete-organizations
  (partial query-delete-models :organizations))

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
