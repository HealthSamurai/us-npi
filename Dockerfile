FROM java:8
EXPOSE 8080

ADD target/usnpi.jar /usnpi.jar

CMD java -cp /usnpi.jar clojure.main -m usnpi.core
