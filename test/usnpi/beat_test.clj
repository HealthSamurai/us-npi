(ns usnpi.beat-test
  (:require [clojure.test :refer :all]
            [usnpi.db :as db]
            [usnpi.error :refer [error!]]
            [usnpi.beat :as beat]
            [usnpi.tasks :as tasks]
            [usnpi.time :as time]))

(defn task-test-ok []
  (/ 2 1))

(defn task-test-err []
  (error! "Failure example"))

(deftest test-beat-start-stop
  (when (beat/status)
    (beat/stop))

  (is (not (beat/status)))

  (beat/start)
  (is (beat/status))
  (is (thrown? Exception (beat/start)))

  (beat/stop)
  (is (not (beat/status)))
  (is (thrown? Exception (beat/stop))))


(deftest test-beat-scheduler
  (db/execute! "truncate tasks")

  (tasks/seed-task "usnpi.beat-test/task-test-ok" 60)
  (tasks/seed-task "usnpi.beat-test/task-test-err" 60)

  (when-not (beat/status) (beat/start))

  (Thread/sleep (* 1000 1))

  (let [tasks (db/query "select * from tasks order by id")
        [task1 task2] tasks]

    (-> task1 :success is)

    (-> task2 :success not is)
    (-> task2 :message (= "java.lang.Exception: Failure example") is)

    (is (= (compare (:next_run_at task1) (time/now)) 1))
    (is (= (compare (:next_run_at task2) (time/now)) 1))

    (db/execute! "truncate tasks"))

  (beat/stop))
