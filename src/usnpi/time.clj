(ns usnpi.time
  "Misc time wrappers."
  (:require [clj-time.core :as t]))

(defn next-time
  "Adds some amount of seconds to the current time."
  [secs]
  (t/plus (t/now) (t/seconds secs)))

(defn epoch
  "UNIX timestamp in seconds as integer."
  []
  (quot (System/currentTimeMillis) 1000))
