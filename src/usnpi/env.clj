(ns usnpi.env
  (:require [cprop.source :refer [from-env]]
            [usnpi.error :refer [error!]]
            [clojure.spec.alpha :as s]
            [usnpi.spec :as spec]))

(def env (from-env))

;;
;; Init part
;;

(defn init
  []
  (let [spec :usnpi.spec/env]
    (when-not (s/valid? spec env)
      (error! "Wrong config file, check %s declaration." spec))))
