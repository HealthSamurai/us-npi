(ns usnpi.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core :as json]
            [ring.mock.request :as mock]
            [usnpi.db :as db]
            [usnpi.beat :as beat]
            [usnpi.core :as usnpi]
            [usnpi.tasks :as tasks]
            [usnpi.time :as time]
            [usnpi.models :as models]))

(defn- read-json
  [res]
  (json/parse-string (:body res) true))

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

(defn task-test-err []
  (/ 2 0))

(deftest test-beat-scheduler
  (db/execute! "truncate tasks")

  (tasks/seed-task "usnpi.core-test/task-test-ok" 60)
  (tasks/seed-task "usnpi.core-test/task-test-err" 60)

  (when-not (beat/status) (beat/start))

  (Thread/sleep (* 1000 1))

  (let [tasks (db/query "select * from tasks order by id")
        [task1 task2] tasks]

    (-> task1 :success is)

    (-> task2 :success not is)
    (-> task2 :message (= "java.lang.ArithmeticException: Divide by zero") is)

    (is (= (compare (:next_run_at task1) (time/now)) 1))
    (is (= (compare (:next_run_at task2) (time/now)) 1))

    (db/execute! "truncate tasks"))

  (beat/stop))

(deftest test-model-api

  (db/execute! "truncate practitioner")
  (db/execute! "truncate organizations")

  (let [models (-> "npi_sample.csv" io/resource io/input-stream models/read-models)
        practs (filter models/practitioner? models)
        orgs (filter models/organization? models)]

    (db/insert-multi! :practitioner (map db/model->row practs))
    (db/insert-multi! :organizations (map db/model->row orgs)))

  (testing "Single practitioner"
    (let [npi-pract "1932601184"
          url-pract-ok (format "/practitioner/%s" npi-pract)
          url-pract-err (format "/practitioner/%s" "1010101010101")]

      (testing "OK"
        (let [res (usnpi/index (mock/request :get url-pract-ok))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :name first :given first)
                 "YU"))))

      (testing "Missing"
        (let [res (usnpi/index (mock/request :get url-pract-err))]
          (is (= (:status res) 404))))

      (testing "Deleted"
        (db/execute! ["update practitioner set deleted = true where id = ?" npi-pract])
        (let [res (usnpi/index (mock/request :get url-pract-ok))]
          (is (= (:status res) 404))))))

  (testing "Practitioner search"
    (let [url "/practitioner"]

      (testing "Test limit"
        (let [res (usnpi/index (mock/request :get url {:_count 3}))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :entry count) 3)))

        (let [res (usnpi/index (mock/request :get url ))]
          (is (= (:status res) 200))
          (is (> (-> res read-json :entry count) 3))))

      (testing "Query term"
        (let [res (usnpi/index (mock/request :get url {:q "david"}))]
          (is (= (:status res) 200))
          (is (-> res read-json :entry not-empty)))

        (let [res (usnpi/index (mock/request :get url {:q "g:david"}))]
          (is (= (:status res) 200))
          (is (-> res read-json :entry not-empty)))

        (let [res (usnpi/index (mock/request :get url {:q "c:new"}))]
          (is (= (:status res) 200))
          (is (-> res read-json :entry not-empty)))

        (let [res (usnpi/index (mock/request :get url {:q "g:David c:Roger"}))]
          (is (= (:status res) 200))
          (is (-> res read-json :entry not-empty))))))

  (testing "Practitioner batch"
    (let [url "/practitioner/$batch"
          id1 "1669586954"
          id2 "1760859052"
          id3 "lalilulelo"
          ids (str/join "," [id1, id2, id3])]

      (db/update! :practitioner {:deleted true} ["id = ?" id1])

      (testing "Ignores deleted and missing"
        (let [res (usnpi/index (mock/request :get url {:ids ids}))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :entry count) 1))))))

  (testing "Single organization"
    (let [id "1932315942"
          url-ok (format "/organization/%s" id)
          url-err "/organization/dunno"]

      (testing "OK"
        (let [res (usnpi/index (mock/request :get url-ok))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :address first :city) "SAINT LOUIS"))))

      (testing "Missing"
        (let [res (usnpi/index (mock/request :get url-err))]
          (is (= (:status res) 404))))

      (testing "Deleted"
        (db/update! :organizations {:deleted true} ["id = ?" id])
        (let [res (usnpi/index (mock/request :get url-ok))]
          (is (= (:status res) 404))))))

  (testing "Organization batch"
    (let [url "/organization/$batch"
          id1 "1578065728"
          id2 "1295237444"
          id3 "lalilulelo"
          ids (str/join "," [id1 id2 id3])]

      (db/update! :organizations {:deleted true} ["id = ?" id1])

      (testing "OK"
        (let [res (usnpi/index (mock/request :get url {:ids ids}))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :entry count) 1))))

      (testing "no ids"
        (let [res (usnpi/index (mock/request :get url ))]
          (is (= (:status res) 400))))))

  (testing "Organization search"
    (let [url "/organization"]

      (testing "OK"
        (let [res (usnpi/index (mock/request :get url {:q "physical"}))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :entry count) 2))))

      (testing "prefixes"
        (let [res (usnpi/index (mock/request :get url {:q "n:SCHUS c:JEFF"}))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :entry count) 1))))

      (testing "Limit"
        (let [res (usnpi/index (mock/request :get url {:_count 3}))]
          (is (= (:status res) 200))
          (is (= (-> res read-json :entry count) 3)))))))
