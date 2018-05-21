#!/bin/bash

java -cp htrc-cassandra-ingester-jar-with-dependencies.jar edu.indiana.d2i.delete.CassandraVolumeRemover delete-ids.txt removed-ids.txt