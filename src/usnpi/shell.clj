(ns usnpi.shell
  (:require [clojure.java.shell :as shell]
            [usnpi.error :refer [error!]]))

(defmacro with-env
  "The same as `clojure.java.shell/with-sh-env` but updates
  an existing ENV rather than substitute it completely."
  [env & forms]
  `(let [env-old# (into {} (System/getenv))
         env-new# (merge env-old# ~env)]
     (shell/with-sh-env env-new#
       ~@forms)))

(defn sh
  "The same as `clojure.java.shell/sh` but raises an exception
  in case of non-zero exit code. Otherwise, returns the output string."
  [& args]
  (let [{:keys [exit out err]} (apply shell/sh args)]
    (if (= exit 0)
      out
      (error! "Shell error: code %s, reason: %s" exit err))))
