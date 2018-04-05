(ns usnpi.format-test
  (:require [clojure.test :refer :all]
            [ring.mock.request :as mock]
            [usnpi.util-test :refer [read-body]]
            [usnpi.core :as usnpi]))

(deftest test-format

  (let [url "/metadata"
        ct "Content-Type"]

    (testing "JSON by default"
      (let [req (mock/request :get url)
            res (usnpi/app req)]
        (is (= (:status res) 200))
        (is (= (-> res :headers (get ct)) "application/json"))))

    (testing "YAML"
      (let [req (mock/request :get url {:_format "yaml"})
            res (usnpi/app req)]
        (is (= (:status res) 200))
        (is (= (-> res :headers (get ct)) "text/yaml"))))

    (testing "EDN"
      (let [req (mock/request :get url {:_format "edn"})
            res (usnpi/app req)]
        (is (= (:status res) 200))
        (is (= (-> res :headers (get ct)) "text/edn"))))))
