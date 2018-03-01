(ns usnpi.tasks
  (:require [usnpi.db :as db]
            [usnpi.beat :as beat]
            [usnpi.update :as update]
            [clojure.tools.logging :as log]))

(defn- func->str
  "Turns a var reference into a full-qualified string."
  [ref]
  (let [m (meta ref)]
    (format "%s/%s" (ns-name (:ns m)) (:name m))))

(def ^:private
  hour (* 60 60))

(def ^:private
  tasks

  [{:handler (func->str #'update/task-deactivation)
    :interval (* hour 6)
    :offset 0}

   {:handler (func->str #'update/task-dissemination)
    :interval (* hour 6)
    :offset hour}])

(defn init []
  (log/info "Seeding regular tasks...")
  (doseq [{:keys [handler interval offset]} tasks]
    (when-not (beat/task-exists? handler)
      (let [db-task (beat/seed-task handler interval offset)
            {:keys [id next_run_at]} db-task]
        (log/infof "Task %s with DB ID %s scheduled on %s"
                   handler id next_run_at))))
  (log/info "Tasks done.")
  (log/info "Starting beat...")
  (beat/start))
