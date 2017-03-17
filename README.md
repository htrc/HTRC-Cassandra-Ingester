# HTRC-Cassandra-Ingester

A tool to read volumes from a pairtree structure on the file system, and push
volume content and metadata into Cassandra. The tool expects as input, a list
of volume identifiers for which data needs to be ingested into Cassandra. The
ingestion works in two phases. In the first phase, volume zip files and
structural metadata (METS) for the given volumes are found at pairtree
leaves, and ingested into Cassandra. In the second phase, the semantic
metadata (MARC) for the given volumes are found in specified files, and
existing rows in Cassandra are updated with the MARC records.

## Build

Create an executable jar containing all dependencies, i.e., a fat jar, using
the following command:
```
mvn clean package
```

## Run
```
java -cp htrc-cassandra-ingester.jar edu.indiana.d2i.ingest.IngestService
```

## Configuration

HTRC-Cassandra-Ingester loads configuration settings from a file,
conf/conf.properties. An example of this file is shown below:

```
#ingest config
SOURCE = pairtree
####cassandra configuration####
CONTACT_POINTS = crow.soic.indiana.edu,pipit.soic.indiana.edu,vireo.soic.indiana.edu
KEY_SPACE = HTRCCorpus2
VOLUME_TEXT_COLUMN_FAMILY = VolumeContents

MARC-JSON-FILES-FOLDER=/hathitrustmnt/marc-ingester
MARC-JSON-FILES=meta_pd_open_access.json,meta_pd_google.json,meta_ic.json
MARC-COLUMN-FAMILY=marctest
MARC-COLUMN-FAMILY-KEY=volumeid
MARC-COLUMN=marc
MARC-INGESTER-OUTFILE=marc-ingester-output.txt
VOLUME_ID_LIST=test-csd-vol-ids.txt
```

| Property | Description |
| --- | --- |
| CONTACT_POINTS | Nodes in the Cassandra cluster |
| KEY_SPACE | Name of keyspace in Cassandra. The keyspace should have been created before HTRC-Cassandra-Ingester is run. |
| VOLUME_TEXT_COLUMN_FAMILY | Name of the column family that stores volume content. If this column family does not exist, it is created by HTRC-Cassandra-Ingester. |
| MARC-JSON-FILES-FOLDER | Folder containing files with MARC records in JSON format |
| MARC-JSON-FILES | Comma-separated list of files containing MARC records in JSON format |
| MARC-COLUMN-FAMILY | Name of the column family that stores MARC records; at present, this is the same as VOLUME_TEXT_COLUMN_FAMILY |
| MARC-COLUMN-FAMILY-KEY | Key with which MARC records in MARC-COLUMN-FAMILY are updated |
| MARC-COLUMN | Name of column in MARC-COLUMN-FAMILY that contains MARC records |
| MARC-INGESTER-OUTFILE | Name of output file created in the MARC ingestion phase |
| VOLUME_ID_LIST | Text file containing the list of volume identifiers for which ingestion is performed, one volume identifier per line |

## Output

There are two output files.

- failedChecksumVolIds.txt: List of volume identifiers for which there were mismatches between the checksum provided in the structural metadata (METS) and the checksum calculated by HTRC-Cassandra-Ingester. Coming soon.
- marc-ingester-output.txt: Information about errors during the MARC ingestion phase, e.g., volumes for which MARC records were not found.
