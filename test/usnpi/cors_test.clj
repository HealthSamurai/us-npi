(ns usnpi.cors-test
  (:require [clojure.test :refer :all]
            [ring.mock.request :as mock]
            [usnpi.core :as usnpi]))

(deftest test-cors

  (testing "options"
    (let [req (-> :options
                  (mock/request "/")
                  (mock/header "Origin" "http://api.bob.com")
                  (mock/header "Access-Control-Request-Method" "PUT")
                  (mock/header "Access-Control-Request-Headers" "X-Custom-Header"))
          res (usnpi/app req)]
      (is (= (:status res) 200))
      (is (= (-> res :headers (get "Access-Control-Allow-Headers"))
             "X-Custom-Header"))
      (is (= (-> res :headers (get "Access-Control-Allow-Methods"))
             "PUT"))
      (is (= (-> res :headers (get "Access-Control-Allow-Origin"))
             "http://api.bob.com"))))

  (testing "get"
    (let [req (-> :get
                  (mock/request "/")
                  (mock/header "Origin" "http://api.bob.com"))
          res (usnpi/app req)]
      (is (= (:status res) 200))
      (is (= (-> res :headers (get "Access-Control-Allow-Origin"))
             "http://api.bob.com"))
      (is (= (-> res :headers (get "Access-Control-Allow-Credentials"))
             "true"))
      (is (= (-> res :headers (get "Access-Control-Expose-Headers"))
             "Location, Content-Location, Category, Content-Type, X-total-count")))))
