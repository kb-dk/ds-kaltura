# ds-kaltura

**Description of the application goes here**

Developed and maintained by the Royal Danish Library.

## Requirements

* Maven 3                                  
* Java 17

## Setup

**PostgreSQL database creation, Solr installation etc. goes here**

## Build & run

Build with standard
```
mvn package
```

This produces `target/ds-kaltura-<version>-distribution.tar.gz` which contains JARs, configurations and
`start-script.sh` for running the application. 

Quick development testing can be done by calling
```shell
target/ds-kaltura-*-SNAPSHOT-distribution/ds-kaltura-*-SNAPSHOT/bin/start-script.sh
```

See the file [DEVELOPER.md](DEVELOPER.md) for developer specific details.
