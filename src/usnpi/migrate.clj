(ns usnpi.migrate
  (:require [usnpi.db :as db]
            [clojure.tools.logging :as log]))

(def ^:private
  migrations
  [
   "
create table if not exists tasks (
    id          serial primary key,
    handler     text not null,
    interval    integer not null default 0,
    last_run_at timestamp with time zone null,
    next_run_at timestamp with time zone not null,
    success     boolean not null default false,
    message     text not null default ''
);"

   "
alter table practitioner
    add column if not exists
    deleted boolean not null default false;
"
   ])

(defn migrate []
  (log/info "Running migrations...")

  (doseq [[i mig] (map-indexed vector migrations)]
    (log/infof "Migration: %s" i)
    (db/execute! mig))

  (log/info "Done."))
