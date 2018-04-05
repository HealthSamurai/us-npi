(ns usnpi.swagger-test
  (:require [clojure.test :refer :all]
            [usnpi.util-test :refer [read-body]]
            [ring.mock.request :as mock]
            [usnpi.core :as usnpi]))

(deftest test-swagger

  (testing "Index page"
    (let [res (usnpi/app (mock/request :get "/swagger"))]
      (is (= (:status res) 200))))

  (testing "Schema"
    (let [res (usnpi/app (mock/request :get "/swagger/schema"))]
      (is (= (:status res) 200))
      (is (= (-> res read-body :swagger) "2.0")))))
