(ns usnpi.beat
  (:require [usnpi.db :as db]
            [clojure.tools.logging :as log]))

(defn- raise!
  ([msg]
   (throw (Exception. msg)))
  ([tpl & args]
   (raise! (apply format tpl args))))

(defn- epoch []
  (quot (System/currentTimeMillis) 1000))

(defn- get-handler [task]
  (-> task :task symbol resolve))

(defn- get-next-time [task]
  (+ (epoch) (:interval task)))

(defn- update-task [task fields]
  (db/update! :tasks fields ["task = ?" (:task task)]))

(defn- task-running [task]
  (update-task task {:message "Task is running..."
                     :last_run_at (epoch)}))

(defn- task-success [task]
  (update-task task {:success true
                     :message "Successfully run."
                     :next_run_at (get-next-time task)}))

(defn- exc-msg [e]
  (let [class (-> e .getClass .getCanonicalName)
        message (-> e .getMessage (or "<no message>"))]
    (format "Exception: %s %s" class message)))

(defn- task-failure [task e]
  (update-task task {:success false
                     :message (exc-msg e)
                     :next_run_at (get-next-time task)}))

(defn- get-task [task]
  (first (db/find-by-keys :tasks {:task (:task task)})))

(defn- create-task [task]
  (db/insert! :tasks {:task (:task task)
                      :message "Task created."
                      :next_run_at (get-next-time task)}))

(defn- run-task [task]
  (if-let [handler (get-handler task)]
    (do
      (log/infof "Starting task: %s" (:task task))
      (task-running task)

      (handler)

      (log/infof "Task is done: %s" (:task task))
      (task-success task)
      nil)

    (raise! "Cannot resolve a task: %s" (:task task))))

(defn- process-task [task]
  (let [row (get-task task)]
    (cond

      (nil? row)
      (do
        (create-task task)
        (run-task task))

      (< (:next_run_at row) (epoch))
      (run-task task))))

;;
;; beat
;;

(def ^:private
  state (atom false))

(def ^:private
  timeout (* 1000 60 10))

(defn- beat [tasks]
  (while @state
    (doseq [task tasks]
      (future
        (try
          (process-task task)
          (catch Throwable e
            (log/error e "Uncaught exception")
            (task-failure task e)))))
    (Thread/sleep timeout)))

;;
;; public api
;;

(def default-tasks
  [{:task "usnpi.updater/task"
    :interval (* 60 60)}])

(defn start
  ([]
   (start default-tasks))

  ([tasks]
   (when @state
     (raise! "The schedule beat has been already run."))
   (reset! state true)
   (log/info "Beat started.")
   (future (beat tasks))
   nil))

(defn stop []
  (reset! state false)
  (log/info "Beat stopped.")
  nil)
