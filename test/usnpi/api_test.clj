(ns usnpi.api-test
  (:require [clojure.test :refer :all]
            [ring.mock.request :as mock]
            [usnpi.core :as usnpi]))

(deftest test-root-page
  (testing "The root page returns the request map"
    (let [req (mock/request :get "/")
          res (usnpi/index req)]
      (is (= (:status res) 200)))))
