(ns usnpi.core-test
  (:require [clojure.test :refer :all]
            [usnpi.db :as db]
            [usnpi.core :as usnpi]
            [usnpi.beat :as beat]
            [usnpi.tasks :as tasks]
            [ring.mock.request :as mock]))

(deftest test-root-page
  (testing "The root page returns the request map"
    (let [req (mock/request :get "/")
          res (usnpi/index req)]
      (is (= (:status res) 200)))))

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

(defn task-test-ok []
  (/ 2 1))

(defn task-test-err[]
  (/ 2 0))

(deftest test-beat-scheduler
  (db/execute! "truncate tasks")

  (tasks/seed-task "usnpi.core-test/task-test-ok" 60)
  (tasks/seed-task "usnpi.core-test/task-test-err" 60)

  (when-not (beat/status) (beat/start))

  (Thread/sleep (* 1000 1))

  (let [tasks (db/query "select * from tasks order by id")]

    (-> tasks first :success is)

    (-> tasks second :success not is)
    (-> tasks second :message (= "java.lang.ArithmeticException: Divide by zero") is)

    (db/execute! "truncate tasks"))

  (beat/stop))
