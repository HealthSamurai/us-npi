(ns usnpi.time
  "Misc time wrappers."
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [cheshire.generate :refer [add-encoder]])
  (:import org.joda.time.DateTime))

(def ^:private
  iso8601 (f/formatters :date-time))

(add-encoder
 DateTime
 (fn [dt jsonGenerator]
   (.writeString jsonGenerator (f/unparse iso8601 dt))))

(defn next-time
  "Adds some amount of seconds to the current time."
  [secs]
  (t/plus (t/now) (t/seconds secs)))

(defn epoch
  "UNIX timestamp in seconds as integer."
  []
  (quot (System/currentTimeMillis) 1000))

(def now t/now)
