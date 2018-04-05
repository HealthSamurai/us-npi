(ns usnpi.api-test
  (:require [clojure.test :refer :all]
            [ring.mock.request :as mock]
            [usnpi.util-test :refer [read-body]]
            [usnpi.core :as usnpi]))

(deftest test-root-page
  (testing "The root page returns the request map"
    (let [req (mock/request :get "/")
          res (usnpi/app req)]
      (is (= (:status res) 200)))))

(deftest test-caps-page
  (testing "The metadata page returns a CapabilityStatement object."
    (let [req (mock/request :get "/metadata")
          res (usnpi/app req)]
      (is (= (:status res) 200))
      (is (= (-> res read-body :resourceType) "CapabilityStatement")))))
