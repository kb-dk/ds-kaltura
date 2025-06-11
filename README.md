# ds-kaltura

Developed and maintained by the Royal Danish Library.

## ⚠️ Warning: Copyright Notice
Please note that it is not permitted to download and/or otherwise reuse content from the DR-archive at The Danish Royal Library.

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


## AppTokens and admin secret

The library contains a script to manage application tokens in kaltura. You can add an application token with

`bin/appTokens.sh add -s '<admin secret> -d'Description of the app token'`

It outputs the token and a token_id which can be used when configuring the kaltura client like this:

`kaltura:
   token: 'xxxx'
  tokenId: 'yyyy'
`

An application token can be deleted with

`bin/appTokens.sh delete -s '<admin secret> -i'<tokenId>'`

Or you can get a list of the current app tokens with:

`bin/appTokens.sh list -s '<admin secret>'`





## Usage as stand-alone-project
Example call for getKulturaInternalId:

`bin/idlookup.sh 67624fe7-b1d9-4225-8afb-e41d8a1190ac`  (the id '67624fe7-b1d9-4225-8afb-e41d8a1190ac' does exist as of 20240412)

Example call for uploadMedia:

`bin/uploadfile.sh /home/digisam/teg/4328a664-403e-4574-960a-8a85195b8e69 -referenceid=test_ref_teg1239 -type=VIDEO -title='title9' -description='description9' -tag='DS-KALTURA'`

Just call`bin/uploadfile.sh` to see usage



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
