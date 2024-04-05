# ds-kaltura

Developed and maintained by the Royal Danish Library.

## About
Ds-kaltura is a library that wraps API calls to Kaltura and can reuse a client session between calls. The java auto generated
Kaltura API is not very user friendly and this is client library makes it easier. As an example will uploading a file to Kaltura and
connect the file to a meta data record will require 4 different API calls. This is wrapped in a single method in this API.

Two metods are implemented so far. Probably more to come...

Methods:
*  String getKulturaInternalId(String referenceId)

Map our own referenceId used when uploading a file to kaltura to the Kaltura internal id.

The internal id seems to be required for showing thumbnails. Also it is the only way we later can 1-1 map our collection to the kaltura collection.

*  String uploadMedia(String filePath,String referenceId,MediaType mediaType,String title,String description, String tag) 

Upload a file to Kaltura. mediaType must be AUDIO or VIDEO. For the DS project we use the tag 'DS-KALTURA' since collections
are mixed in Kaltura with other projects.

The project can both be used as a jar dependency library and as a stand-alone java project with script support.


## Usage as stand-alone-project
Example call for getKulturaInternalId:

`bin/idlookup.sh 0_vvp1ozjl`  (the id '0_vvp1ozjl' does exist as of 20240405)

Example call for uploadMedia:

`bin/uploadfile.sh /home/teg/kaltura_files/video/4328a664-403e-4574-960a-8a85195b8e69 test_ref_teg123 VIDEO 'title' 'en description' DS-KALTURA`



## Requirements
* Java 11 or 17
* Linux OS if used 


## Build & run

Build with standard
```
mvn package
```

This produces `target/ds-kaltura-<version>-distribution.tar.gz` which contains JARs, configurations and
`uploadFile.sh`  or  `idlookup.sh`  for running the application. 

Quick development testing can be done by calling

`target/ds-kaltura-*-SNAPSHOT-distribution/ds-kaltura-*-SNAPSHOT/bin/uploadFile.sh`

or

`target/ds-kaltura-*-SNAPSHOT-distribution/ds-kaltura-*-SNAPSHOT/bin/idlookup.sh`


See the file [DEVELOPER.md](DEVELOPER.md) for developer specific details.
