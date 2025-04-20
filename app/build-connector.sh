#!/bin/bash

apt install git -y
git clone https://github.com/datastax/spark-cassandra-connector.git
cd spark-cassandra-connector
./sbt/sbt assembly

# move built jar to /app to use as connector
cp $(find . -name "spark-cassandra-connector-assembly*.jar" | head -n 1) /app/spark-cassandra-connector.jar
