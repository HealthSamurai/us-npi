FROM java:8
EXPOSE 8080

CMD apt-get install unzip p7zip curl postgresql-client

ENV DATABASE_URL="ups"

ADD target/usnpi.jar /usnpi.jar

CMD java -cp /usnpi.jar clojure.main -m usnpi.core
