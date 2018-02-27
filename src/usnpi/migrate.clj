(ns usnpi.migrate
  (:require [usnpi.db :as db]
            [clojure.tools.logging :as log]))

(def ^:private
  migrations
  [{:name "Add schedule table"
    :sql "
CREATE TABLE IF NOT EXISTS schedule (
    id       serial primary key,
    task     text not null,
    run_last timestamp null,
    run_next timestamp null,
    success  boolean not null default false,
    message  text not null default ''
);"}])

(defn migrate []
  (log/info "Running migrations...")

  (doseq [mig migrations]
    (log/infof "Migration: %s" (:name mig))
    (db/execute! (:sql mig)))

  (log/info "Done."))
