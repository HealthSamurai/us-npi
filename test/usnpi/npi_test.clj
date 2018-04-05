(ns usnpi.npi-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [usnpi.util-test :refer [read-body]]
            [ring.mock.request :as mock]
            [usnpi.db :as db]
            [usnpi.core :as usnpi]
            [usnpi.models :as models]))

(defn fix-load-sample
  [f]
  (db/with-tx-test
    (db/execute! "truncate practitioner, organizations cascade")
    (let [models (-> "npi_sample.csv"
                     io/resource
                     io/input-stream
                     models/read-models)

          practs (filter models/practitioner? models)
          orgs (filter models/organization? models)]

      (db/insert-multi! :practitioner (map db/model->row practs))
      (db/insert-multi! :organizations (map db/model->row orgs)))
    (f)))

(use-fixtures
  :each
  fix-load-sample)

(deftest test-pract-api
  (testing "Single practitioner"
    (let [npi-pract "1932601184"
          url-pract-ok (format "/practitioner/%s" npi-pract)
          url-pract-err (format "/practitioner/%s" "1010101010101")]

      (testing "OK"
        (let [res (usnpi/app (mock/request :get url-pract-ok))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :name first :given first)
                 "YU"))))

      (testing "Aidbox"
        (let [res (usnpi/app (mock/request :get url-pract-ok {:aidbox 1}))]
          (is (= (:status res) 200))))

      (testing "Missing"
        (let [res (usnpi/app (mock/request :get url-pract-err))]
          (is (= (:status res) 404))))

      (testing "Deleted"
        (db/execute! ["update practitioner set deleted = true where id = ?" npi-pract])
        (let [res (usnpi/app (mock/request :get url-pract-ok))]
          (is (= (:status res) 404))))))

  (testing "Practitioner batch"
    (let [url "/practitioner/$batch"
          id1 "1669586954"
          id2 "1760859052"
          id3 "lalilulelo"
          ids (str/join "," [id1, id2, id3])]

      (db/update! :practitioner {:deleted true} ["id = ?" id1])

      (testing "Ignores deleted and missing"
        (let [res (usnpi/app (mock/request :get url {:ids ids}))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :entry count) 1))))))

  (testing "Practitioner search"
    (let [url "/practitioner"]

      (testing "Test limit"
        (let [res (usnpi/app (mock/request :get url {:_count 3}))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :entry count) 3)))

        (let [res (usnpi/app (mock/request :get url))]
          (is (= (:status res) 200))
          (is (> (-> res read-body :entry count) 3))))

      (testing "Aidbox"
        (let [res (usnpi/app (mock/request :get url {:aidbox 1}))]
          (is (= (:status res) 200))))

      (testing "Query term"
        (let [res (usnpi/app (mock/request :get url {:q "david"}))]
          (is (= (:status res) 200))
          (is (-> res read-body :entry not-empty)))

        (let [res (usnpi/app (mock/request :get url {:q "g:david"}))]
          (is (= (:status res) 200))
          (is (-> res read-body :entry not-empty)))

        (let [res (usnpi/app (mock/request :get url {:q "c:new"}))]
          (is (= (:status res) 200))
          (is (-> res read-body :entry not-empty)))

        (let [res (usnpi/app (mock/request :get url {:q "g:David c:Roger"}))]
          (is (= (:status res) 200))
          (is (-> res read-body :entry not-empty)))))))

(deftest test-org-api
  (testing "Single organization"
    (let [id "1932315942"
          url-ok (format "/organization/%s" id)
          url-err "/organization/dunno"]

      (testing "OK"
        (let [res (usnpi/app (mock/request :get url-ok))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :address first :city) "SAINT LOUIS"))))

      (testing "Missing"
        (let [res (usnpi/app (mock/request :get url-err))]
          (is (= (:status res) 404))))

      (testing "Deleted"
        (db/update! :organizations {:deleted true} ["id = ?" id])
        (let [res (usnpi/app (mock/request :get url-ok))]
          (is (= (:status res) 404))))))

  (testing "Organization batch"
    (let [url "/organization/$batch"
          id1 "1578065728"
          id2 "1295237444"
          id3 "lalilulelo"
          ids (str/join "," [id1 id2 id3])]

      (db/update! :organizations {:deleted true} ["id = ?" id1])

      (testing "OK"
        (let [res (usnpi/app (mock/request :get url {:ids ids}))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :entry count) 1))))

      (testing "no ids"
        (let [res (usnpi/app (mock/request :get url ))]
          (is (= (:status res) 400))))))

  (testing "Organization search"
    (let [url "/organization"]

      (testing "OK"
        (let [res (usnpi/app (mock/request :get url {:q "physical"}))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :entry count) 2))))

      (testing "prefixes"
        (let [res (usnpi/app (mock/request :get url {:q "n:SCHUS c:JEFF"}))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :entry count) 1))))

      (testing "Limit"
        (let [res (usnpi/app (mock/request :get url {:_count 3}))]
          (is (= (:status res) 200))
          (is (= (-> res read-body :entry count) 3)))))))
