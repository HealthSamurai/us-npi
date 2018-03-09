(ns usnpi.spec
  (:require [clojure.spec.alpha :as s]))

(s/def ::db-host string?)
(s/def ::db-port int?)
(s/def ::db-database string?)
(s/def ::db-user string?)
(s/def ::db-password string?)

(s/def ::port int?)

(s/def ::api-ops boolean?)

(s/def ::env (s/keys :req-un [::db-host
                              ::db-port
                              ::db-database
                              ::db-user
                              ::db-password
                              ::port
                              ::api-ops]))
