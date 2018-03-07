FROM java:8
EXPOSE 8080

RUN apt-get -y update
RUN apt-get -y install unzip p7zip-full curl postgresql-client

ADD target/usnpi.jar /usnpi.jar

CMD java -cp /usnpi.jar clojure.main -m usnpi.core
