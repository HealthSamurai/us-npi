(ns usnpi.beat
  (:require [usnpi.db :as db]
            [clojure.tools.logging :as log]))

(def ^:private
  state (atom false))

(def ^:private
  timeout (* 1000 60 10))

(defn- error!
  ([msg]
   (throw (Exception. msg)))
  ([tpl & args]
   (error! (apply format tpl args))))

(defn- get-handler [task]
  (-> task :task resolve))

(defn- get-next-time [task]
  (+ 1 2 3)) ;; todo

(defn- update-task [task fields]
  (db/update! :tasks fields ["task = ?" (:task task)]))

(defn- task-success [task]
  (update-task task {:success true
                     :message "Successfully run."
                     :next_run_at (get-next-time task)}))

(defn- task-failure [task e]
  (let [class (-> e .getClass .getCanonicalName)
        message (-> e .getMessage (or "<no message>"))
        explain (format "Exception: %s %s" class message)]
    (update-task task {:success false
                       :message explain
                       :next_run_at (get-next-time task)})))

(defn- get-task [task]
  (first (db/find-by-keys :tasks {:task (:task task)})))

(defn- create-task [task]
  (db/insert! :tasks {:task (:task task)
                      :next_run_at #inst "2017"
                      :message "Task scheduled."}))

(defn- process-task [task]
  (if-let [handler (get-handler task)]
    (handler)
    (error! "Cannot resolve a task: %s" task)))

(defn- beat [tasks]
  (while @state
    (doseq [task tasks]
      (future
        (try
          (process-task task)
          (task-success task)
          (catch Throwable e
            (task-failure task e)))))
    (Thread/sleep timeout)))

;;
;; public part
;;

(def default-tasks
  [{:task "usnpi.updater/task"
    :interval (* 60 60)}])

(defn start
  ([]
   (start default-tasks))

  ([tasks]
   (when @state
     (error! "The schedule beat has been already run."))
   (reset! state true)
   (log/info "Beat started.")
   (future (beat tasks))
   nil))

(defn stop []
  (reset! state false)
  (log/info "Beat stopped.")
  nil)
