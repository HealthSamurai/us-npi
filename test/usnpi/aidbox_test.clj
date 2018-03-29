(ns usnpi.aidbox-test
  (:require [clojure.test :refer :all]
            [usnpi.aidbox :refer [->aidbox]]))

(def sample-input
  {:resourceType "Practitioner"
   :id "1043213226"
   :gender "male"
   :extension
   [{:url "http://some/url/extension.top1"
     :extension
     [{:url "http://some/url/extension.child11" :valueQuantity {:value 999}}
      {:url "http://some/url/extension.child12" :valueString "test"}]}
    {:url "http://some/url/extension.top2"
     :extension
     [{:url "http://some/url/extension.child21" :reference "Organization/12345"}
      {:url "http://some/url/extension.child22" :reference "http://path/to/org/12345"}]}]})

(def sample-output
  {:resourceType "Practitioner"
   :id "1043213226"
   :gender "male"
   "extension.top1"
   {"extension.child11" {:value 999}
    "extension.child12" "test"}
   "extension.top2"
   {"extension.child21" {:id "12345" :resourceType "Organization"}
    "extension.child22" {:uri "http://path/to/org/12345"}}})

(deftest test-transform
  (is (= (->aidbox sample-input) sample-output)))
