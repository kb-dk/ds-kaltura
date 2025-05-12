# Changelog
All notable changes to ds-kaltura will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Kaltura AppToken Client changed to use enum types (values) to start session. This was required since Kaltura was changed.

### Added
- Method to reject a stream in Kaltura. The stream can not be played with rejected status. A KMC moderator can
see all rejected videos in the KMC and change status again if needed.
bumped kb-util to v1.6.8  for consistency.

### Changed

## [1.2.6] (https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.6) - 2024-11-19
### Added
- Method to reject a stream in Kaltura. The stream can not be played with rejected status. A KMC moderator can
see all rejected videos in the KMC and change status again if needed.

## [1.2.5] (https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.5) - 2024-10-15
### Changed
- Fix log typo. 
- When client is refreshed, log how many millis since last refresh 

## [1.2.4] (https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.4) - 2024-10-14
### Changed
- Bump kb-util to v1.5.11
- Client can be created with AppTokens and no longer needs kaltura mastersecret. Creating new AppTokens still require mastersecret

## [1.2.3] (https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.3) - 2024-07-18
- Handle rare error from Kaltura with empty response.

## [1.2.2] (https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.2) - 2024-07-17
- New manuel delete job that takes an input file of kaltura entryId's and deletes each stream+metadata in Kaltura for the entries.

## [1.2.1] (https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.1) - 2024-06-27
### Added
- New method that deletes a stream and all metadata for the stream in Kaltura by the internal Kaltura entryId as key.

### Changed
- Updated kb-util version


## [1.2.0] (https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.0) - 2024-05-28 - 2024-06-21
- Remove the root `.config` from the configuration
- More methods.

## [1.0.0] - YYYY-MM-DD
### Added

- Initial release of <project>


[Unreleased](https://github.com/kb-dk/ds-kaltura/compare/v1.0.0...HEAD)
[1.0.0](https://github.com/kb-dk/ds-kaltura/releases/tag/v1.0.0)
