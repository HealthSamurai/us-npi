(ns usnpi.beat
  "Provides an endless cycle run into a background future.
  On each step, it fetches tasks from the database to be run.
  A task has a `:handler` sting field that points to an existing
  function of zero arguments. E.g: 'my.project/some-function'. Then
  it's resolved and run."
  (:require [usnpi.db :as db]
            [usnpi.error :refer [error!] :as err]
            [usnpi.time :as time]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]))

(defn- resolve-func [task]
  (-> task :handler symbol resolve))

(defn- update-task [task fields]
  (db/update! :tasks fields ["id = ?" (:id task)]))

(defn- task-running
  "Marks a task as being run at the moment."
  [task]
  (update-task task {:message "Task is running..."
                     :last_run_at (t/now)}))

(defn- task-success [task]
  "Marks a task as being finished successfully."
  (let [run-at (-> task :interval time/next-time)]
    (update-task task {:success true
                       :message "Successfully run."
                       :next_run_at run-at})))

(defn- task-failure
  "Marks a task as being failed because of exception."
  [task e]
  (let [run-at (-> task :interval time/next-time)]
    (update-task task {:success false
                       :message (err/exc-msg e)
                       :next_run_at run-at})))

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
