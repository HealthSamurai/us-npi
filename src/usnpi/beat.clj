(ns usnpi.beat
  "Provides an endless cycle run into a background future.
  On each step, it fetches tasks from the database to be run.
  A task has a `:handler` sting field that points to an existing
  function of zero arguments. E.g: 'my.project/some-function'. Then
  it's resolved and run."
  (:require [usnpi.tasks :as tasks]
            [usnpi.error :refer [error!]]
            [clojure.tools.logging :as log]))

(defonce ^:private
  state (atom nil))

(def ^{:private true
       :doc "A number of milliseconds between each beat."}
  timeout
  (* 1000 60 1))

(defn- finished?
  "Checks whether a future was finished no matter successful or not."
  [f]
  (or (future-cancelled? f)
      (future-done? f)))

(defn- beat
  "Starts an endless cycle evaluating tasks in separated futures."
  []
  (while true
    (try
      (doseq [task (tasks/task-list)]
        (future (tasks/task-process task)))
      (catch Throwable e
        (log/error e "Uncaught exception"))
      (finally
        (Thread/sleep timeout)))))

;;
;; Beat API
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

;;
;; Init part
;;

(defn init []
  (start))
