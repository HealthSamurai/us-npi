(ns usnpi.tasks
  (:require [usnpi.db :as db]
            [usnpi.time :as time]
            [usnpi.update :as update]
            [clojure.tools.logging :as log]))

(defn- func->str
  "Turns a var reference into a full-qualified string."
  [ref]
  (let [m (meta ref)]
    (format "%s/%s" (ns-name (:ns m)) (:name m))))

(def ^:private
  minute 60)

(def ^:private
  hour (* 60 60))

(def ^:private
  tasks

  [{:handler (func->str #'update/task-full-dissemination)
    :interval (* hour 6)
    :offset (* minute 5)}

   {:handler (func->str #'update/task-deactivation)
    :interval (* hour 6)
    :offset (* minute 30)}

   {:handler (func->str #'update/task-dissemination)
    :interval (* hour 6)
    :offset (* minute 45)}])

(defn- task-exists?
  [handler]
  (boolean
   (not-empty
    (db/find-by-keys :tasks {:handler handler}))))

(defn- seed-task
  "Adds a new task into the DB. Handler is a string
  that points to a zero-argument function (e.g. 'namespace/function-name').
  Interval is a number of seconds stands for how often the task should be run.
  Offset is an optional number of seconds to shift the first launch time
  and thus prevent tasks from being executed simultaneously."

  ([handler interval]
   (seed-task handler interval 0))

  ([handler interval offset]
   (let [run-at (time/next-time offset)]
     (db/insert! :tasks {:handler handler
                         :interval interval
                         :next_run_at run-at
                         :message "Task created."}))))

(defn init
  "Scans through the declared tasks and adds those of them
  into the database that are missing. Then stars the beat cycle."
  []
  (log/info "Seeding regular tasks...")
  (doseq [{:keys [handler interval offset]} tasks]
    (when-not (task-exists? handler)
      (let [db-task (seed-task handler interval offset)
            {:keys [id next_run_at]} db-task]
        (log/infof "Task %s with DB ID %s scheduled on %s"
                   handler id next_run_at))))
  (log/info "Tasks done."))
