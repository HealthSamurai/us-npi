(ns usnpi.beat
  (:require [usnpi.db :as db]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]))

(defn- raise!
  ([msg]
   (throw (Exception. msg)))
  ([tpl & args]
   (raise! (apply format tpl args))))

(defn- get-handler [task]
  (-> task :handler symbol resolve))

(defn- get-next-time [task]
  (t/plus (t/now) (t/seconds (:interval task))))

(defn- update-task [task fields]
  (db/update! :tasks fields ["id = ?" (:id task)]))

(defn- task-running [task]
  (update-task task {:message "Task is running..."
                     :last_run_at (t/now)}))

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

(defn- process-task [task]
  (if-let [handler (get-handler task)]
    (handler)
    (raise! "Cannot resolve a task: %s" (:handler task))))

;;
;; beat
;;

(defonce ^:private
  state (atom false))

(def ^:private
  timeout (* 1000 10))

(defn- beat []
  (while @state
    (let [sqlmap {:select [:*]
                  :from [:tasks]
                  :where [:< :next_run_at (t/now)]}
          tasks (db/query (db/to-sql sqlmap))]
      (doseq [task tasks]
        (future
          (try
            (task-running task)
            (log/infof "Starting task: %s" (:handler task))
            (process-task task)
            (log/infof "Task is done: %s" (:handler task))
            (task-success task)
            (catch Throwable e
              (log/error e "Uncaught exception")
              (task-failure task e))))))
    (Thread/sleep timeout)))

;;
;; public api
;;

(defn seed-task
  [handler interval]
  (db/insert! :tasks {:handler handler
                      :interval interval
                      :next_run_at (t/now)
                      :message "Task created."}))

(defn start []
  (when @state
    (raise! "The schedule beat has been already run."))
  (reset! state true)
  (log/info "Beat started.")
  (future (beat))
  nil)

(defn stop []
  (reset! state false)
  (log/info "Beat stopped.")
  nil)
