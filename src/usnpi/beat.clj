(ns usnpi.beat
  (:require [usnpi.db :as db]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]))

(defn- raise!
  ([msg]
   (throw (Exception. msg)))
  ([tpl & args]
   (raise! (apply format tpl args))))

(defn- resolve-func [task]
  (-> task :handler symbol resolve))

(defn- next-time [secs]
  (t/plus (t/now) (t/seconds secs)))

(defn- update-task [task fields]
  (db/update! :tasks fields ["id = ?" (:id task)]))

(defn- task-running [task]
  (update-task task {:message "Task is running..."
                     :last_run_at (t/now)}))

(defn- task-success [task]
  (update-task task {:success true
                     :message "Successfully run."
                     :next_run_at (-> task :interval next-time)}))

(defn- exc-msg [e]
  (let [class (-> e .getClass .getCanonicalName)
        message (-> e .getMessage (or "<no message>"))]
    (format "Exception: %s %s" class message)))

(defn- task-failure [task e]
  (update-task task {:success false
                     :message (exc-msg e)
                     :next_run_at (-> task :interval next-time)}))

(defn- read-tasks []
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

(def ^:private
  timeout (* 1000 60 30))

(defn- finished? [f]
  (or (future-cancelled? f)
      (future-done? f)))

(defn- beat []
  (while true
    (try
      (doseq [{:keys [handler] :as task} (read-tasks)]
        (future
          (try
            (log/infof "Starting task: %s" handler)
            (task-running task)
            (if-let [func (resolve-func task)]
              (func)
              (raise! "Cannot resolve a task: %s" handler))
            (log/infof "Task is done: %s" handler)
            (task-success task)
            (catch Throwable e
              (log/error e "Task error: %s" handler)
              (task-failure task e)))))
      (catch Throwable e
        (log/error e "Uncaught exception"))
      (finally
        (Thread/sleep timeout)))))

;;
;; public api
;;

(defn seed-task
  [handler interval]
  (db/insert! :tasks {:handler handler
                      :interval interval
                      :next_run_at (next-time interval)
                      :message "Task created."}))

(defn start []
  (let [f @state]
    (if (or (nil? f) (and (future? f) (finished? f)))
      (do
        (reset! state (future (beat)))
        (log/info "Beat started."))

      (raise! "The beat wasn't stopped properly."))))

(defn stop []
  (let [f @state]
    (if (and (future? f) (not (finished? f)))
      (do
        (future-cancel f)
        (log/info "Beat stopped."))

      (raise! "The beat was not started or is already stopped."))))
