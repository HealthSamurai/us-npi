(ns usnpi.beat
  "Provides an endless cycle run into a background future.
  On each step, it fetches tasks from the database to be run.
  A task has a `:handler` sting field that points to an existing
  function of zero arguments. E.g: 'my.project/some-function'. Then
  it's resolved and run."
  (:require [usnpi.db :as db]
            [usnpi.util :refer [error!]]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]))

(defn- resolve-func [task]
  (-> task :handler symbol resolve))

(defn- +seconds
  [time secs]
  (t/plus time (t/seconds secs)))

(defn- next-time [secs]
  (+seconds (t/now) secs))

(defn- update-task [task fields]
  (db/update! :tasks fields ["id = ?" (:id task)]))

(defn- task-running
  "Marks a task as being run at the moment."
  [task]
  (update-task task {:message "Task is running..."
                     :last_run_at (t/now)}))

(defn- task-success [task]
  "Marks a task as being finished successfully."
  (update-task task {:success true
                     :message "Successfully run."
                     :next_run_at (-> task :interval next-time)}))

(defn- ^String exc-msg
  "Returns a message string for an exception instance."
  [^Exception e]
  (let [class (-> e .getClass .getCanonicalName)
        message (-> e .getMessage (or "<no message>"))]
    (format "Exception: %s %s" class message)))

(defn- task-failure
  "Marks a task as being failed because of exception."
  [task e]
  (update-task task {:success false
                     :message (exc-msg e)
                     :next_run_at (-> task :interval next-time)}))

(defn- read-tasks
  "Returns all the tasks from the database needed to be run."
  []
  (db/query
   (db/to-sql
    {:select [:*]
     :from [:tasks]
     :where [:< :next_run_at (t/now)]})))

;;
;; beat
;;

(defonce ^:private
  state (atom nil))

(def ^{:private true
       :doc "A number of seconds between each beat."}
  timeout (or (:beat-timeout env) (* 60 30)))

(defn- finished?
  "Checks whether a future was finished no matter successful or not."
  [f]
  (or (future-cancelled? f)
      (future-done? f)))

(defn- beat
  "Starts an endless cycle evaluating tasks in separated futures."
  []
  (while true
    (log/info "Anoter beat cycle...")
    (try
      (doseq [{:keys [handler] :as task} (read-tasks)]
        (future
          (try
            (log/infof "Starting task: %s" handler)
            (task-running task)
            (if-let [func (resolve-func task)]
              (func)
              (error! "Cannot resolve a task: %s" handler))
            (log/infof "Task is done: %s" handler)
            (task-success task)
            (catch Throwable e
              (log/error e "Task error: %s" handler)
              (task-failure task e)))))
      (catch Throwable e
        (log/error e "Uncaught exception"))
      (finally
        (Thread/sleep (* 1000 timeout))))))

;;
;; public api
;;

(defn task-exists?
  [handler]
  (boolean
   (not-empty
    (db/find-by-keys :tasks {:handler handler}))))

(defn seed-task
  "Adds a new task into the DB. Handler is a string
  that points to a zero-argument function (e.g. 'namespace/function-name').
  Interval is a number of seconds stands for how often the task should be run.
  Offset is an optional number of seconds to shift the first launch time
  and thus prevent tasks' simultaneous execution."

  ([handler interval]
   (seed-task handler interval 0))

  ([handler interval offset]
   (let [run-at (next-time (+ interval offset))]
     (db/insert! :tasks {:handler handler
                         :interval interval
                         :next_run_at run-at
                         :message "Task created."}))))

(defn status []
  "Checks whether the beat works or not."
  (let [f @state]
    (and (future? f) (not (finished? f)))))

(defn start []
  "Starts the beat in background."
  (if-not (status)
    (do
      (reset! state (future (beat)))
      (log/info "Beat started."))
    (error! "The beat wasn't stopped properly.")))

(defn stop []
  "Stops the background beat."
  (if (status)
    (do
      (future-cancel @state)
      (log/info "Beat stopped."))
    (error! "The beat was not started or is already stopped.")))
