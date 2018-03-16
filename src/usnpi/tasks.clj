(ns usnpi.tasks
  "Tasks tools and APIs."
  (:require [usnpi.db :as db]
            [usnpi.time :as time]
            [usnpi.error :as error]
            [usnpi.update :as upd]
            [usnpi.error :refer [error!]]
            [usnpi.warmup :as wm]
            [clojure.tools.logging :as log]))

;;
;; Task API
;;

(defn- task-resolve
  "Returns a Clojure function associated with that task. Or nil otherwise."
  [task]
  (-> task :handler symbol resolve))

(defn- task-update [task fields]
  (let [run-at (-> task :interval time/next-time)
        params (assoc fields :next_run_at run-at)]
    (db/update! :tasks params ["id = ?" (:id task)])))

(defn- task-running
  "Marks a task as being run at the moment."
  [task]
  (task-update task {:message "Task is running..."
                     :last_run_at (time/now)}))

(defn- task-success [task]
  "Marks a task as being finished successfully."
  (task-update task {:success true
                     :message "Successfully run."}))

(defn- task-failure
  "Marks a task as being failed because of exception."
  [task e]
  (task-update task {:success false
                     :message (error/exc-msg e)}))

(defn task-list
  "Returns all the tasks from the database needed to be run."
  []
  (db/query "select * from tasks where next_run_at < current_timestamp"))

(defn task-process
  "Runs a task tracking its state."
  [{:keys [handler] :as task}]
  (try
    (log/infof "Starting task: %s" handler)
    (task-running task)
    (if-let [func (task-resolve task)]
      (func)
      (error! "Cannot resolve a task: %s" handler))
    (log/infof "Task is done: %s" handler)
    (task-success task)
    (catch Throwable e
      (log/errorf e "Task error: %s" handler)
      (task-failure task e))))

(defn- task-exists?
  [handler]
  (boolean
   (not-empty
    (db/find-by-keys :tasks {:handler handler}))))

;;
;; Seeding tasks
;;

(def ^:private
  minute 60)

(def ^:private
  hour (* 60 60))

(defn- func->str
  "Turns a var reference into a full-qualified string."
  [ref]
  (let [m (meta ref)]
    (format "%s/%s" (ns-name (:ns m)) (:name m))))

(def ^:private
  tasks

  ;; start the full import in five minutes, might take about 1h
  [{:handler (func->str #'upd/task-full-dissemination)
    :interval (* hour 6)
    :offset (* minute 5)}

   {:handler (func->str #'upd/task-deactivation)
    :interval (* hour 6)
    :offset (+ hour (* minute 30))}

   {:handler (func->str #'upd/task-dissemination)
    :interval (* hour 6)
    :offset (+ hour (* minute 40))}

   {:handler (func->str #'wm/task-warmup-index)
    :interval (* minute 10)
    :offset minute}])

(defn seed-task
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

(defn- seed-tasks
  "Scans through the declared tasks and adds those of them
  into the database that are missing."
  []
  (doseq [{:keys [handler interval offset]} tasks]
    (when-not (task-exists? handler)
      (let [db-task (seed-task handler interval offset)
            {:keys [id next_run_at]} db-task]
        (log/infof "Task %s with DB ID %s scheduled on %s"
                   handler id next_run_at)))))

;;
;; Init
;;

(defn init
  []
  (log/info "Seeding regular tasks...")
  (seed-tasks)
  (log/info "Tasks done."))
