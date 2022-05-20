#!/bin/bash

java -Ddatastax-java-driver.basic.request.timeout="30 seconds" -cp htrc-cassandra-ingester-jar-with-dependencies.jar edu.indiana.d2i.ingest.IngestService 
