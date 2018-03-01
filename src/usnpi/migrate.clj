(ns usnpi.migrate
  (:require [usnpi.db :as db]
            [clojure.tools.logging :as log]
            [migratus.core :as migratus]))

(def ^:private
  config {:store :database
          :migration-dir "migrations/"
          :db db/*db*})

;;
;; public
;;

(defn migrate []
  (log/info "Running migrations...")
  (migratus/migrate config)
  (log/info "Migrations done."))
