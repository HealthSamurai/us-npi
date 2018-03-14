(ns usnpi.core-test
  (:require [clojure.test :refer :all]
            [usnpi.core :as usnpi]
            [ring.mock.request :as mock]))

(deftest core-test
  (let [req (mock/request :get "/")
        res (usnpi/index req)]
    (is (= (:status res) 200))))
