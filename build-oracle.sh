#!/bin/bash
cd /local/home/bahndutr/workplace/aws-athena-query-federation/athena-oracle

# Use Docker to run Maven with newer version
docker run --rm -v "$(pwd)":/usr/src/mymaven -v "$HOME/.m2":/root/.m2 -w /usr/src/mymaven maven:3.8-openjdk-11 mvn clean install -Dcheckstyle.skip=true -DskipTests=true

# Build Docker image
cd ..
docker build -f athena-oracle/Dockerfile -t athena-oracle-connector .
