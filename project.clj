(defproject usnpi "0.1.0-SNAPSHOT"
  :description "US NPI registry in FHIR"
  :url "https://npi.health-samurai.io/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [cheshire "5.6.3"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [http-kit "2.2.0"]
                 [clj-time "0.14.2"]
                 [clj-yaml "0.4.0"]
                 [cprop "0.1.11"]
                 [clj-http "3.7.0"]
                 [cheshire "5.6.3"]
                 [ring/ring-codec "1.1.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.postgresql/postgresql "9.4.1211.jre7"]
                 [migratus "1.0.6"]
                 [org.clojure/data.csv "0.1.4"]
                 [dk.ative/docjure "1.12.0"]
                 [hickory "0.7.1"]
                 [honeysql "0.9.1"]
                 [org.apache.commons/commons-compress "1.5"]
                 [route-map "0.0.6"]

                 [ring-webjars "0.2.0"]
                 [org.webjars/swagger-ui "3.13.0"]]

  :plugins [[migratus-lein "0.5.7"]]

  :migratus {:store :database
             :db {:dbtype   "postgresql"
                  :host     ~(get (System/getenv) "DB_HOST")
                  :port     ~(get (System/getenv) "DB_PORT")
                  :dbname   ~(get (System/getenv) "DB_DATABASE")
                  :user     ~(get (System/getenv) "DB_USER")
                  :password ~(get (System/getenv) "DB_PASSWORD")}}

  :main usnpi.core

  :profiles {:uberjar {:aot :all :omit-source true}

             :test {:dependencies [[ring/ring-mock "0.3.2"]]
                    :resource-paths ["profiles/test/resources"]}}

  :uberjar-name "usnpi.jar")
