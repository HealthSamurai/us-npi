(defproject usnpi "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [cheshire "5.6.3"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.clojure/data.csv "0.1.3"]
                 [http-kit "2.2.0"]
                 [cheshire "5.6.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.postgresql/postgresql "9.4.1211.jre7"]
                 [migratus "1.0.6"]
                 [dk.ative/docjure "1.12.0"]
                 [hickory "0.7.1"]
                 [honeysql "0.9.1"]
                 [ring "1.5.1"]
                 [route-map "0.0.6"]
                 [clj-pg "0.0.3"]
                 [clj-yaml "0.4.0"]
                 [matcho "0.1.0-RC6"]
                 [clojurewerkz/elastisch "3.0.0-beta2"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [ring/ring-defaults "0.2.3"]]

  :plugins [[migratus-lein "0.5.7"]]

  :migratus {:store :database
             :db ~(get (System/getenv) "DATABASE_URL")}

  :main usnpi.core

  :profiles {:uberjar {:aot :all :omit-source true}}
  :uberjar-name "usnpi.jar")
