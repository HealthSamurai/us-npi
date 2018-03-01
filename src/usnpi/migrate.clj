(ns usnpi.migrate
  "Small module to deal with migrations. These are .sql files
  put into resources/migrations folder. Prepend files' names with
  a prefix for proper ordering."
  (:require [usnpi.db :as db]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(defn- file-name
  [^java.io.File file]
  (.getName file))

(defn- is-sql?
  [^java.io.File file]
  (-> file file-name (s/ends-with? ".sql")))

(defn- to-migration
  [^java.io.File file]
  {:name (file-name file)
   :body (slurp file)})

(defn- read-migrations
  [^String res]
  (if-let [mig-dir (-> res io/resource io/file)]
    (->> mig-dir
         file-seq
         (filter is-sql?)
         (sort-by file-name)
         (map to-migration))
    (throw (Exception.
            (format "Cannot read migrations from: %s" res)))))

;;
;; public
;;

(defn migrate []
  (log/info "Running migrations...")

  (doseq [{:keys [name body]} (read-migrations "migrations")]
    (log/infof "Migration: %s" name)
    (db/execute! body))

  (log/info "Done."))
