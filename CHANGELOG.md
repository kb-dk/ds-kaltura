# Changelog

All notable changes to ds-kaltura will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added option to specify and use conversionProfile when uploading. This enables transcoding in Kaltura.

### Changed
- int conversionQueueThreshold, int conversionQueueRetryDelaySeconds is now needed to initialize kalturaClient.
- Removed ability to upload directly under a specified flavorParamId. FlavorParamId can no longer be used when
  uploading.

### Fixed

## [3.0.3](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-3.0.3) - 2025-12-08

### Changed
- Upload now requires file extension to be set separately. This insures that file always have a file extension when
  uploaded.

## [3.0.2](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-3.0.2) - 2025-09-01

### Changed

- DsKalturaClient is now split into two classes. An abstract class called DsKalturaClientBase and DsKalturaClient
  that extends DsKalturaClientBase. DsKalturaClientBase maintains kaltura sessions, generically builds and sends API
  requests to kaltura and unpacks API responses. This serves to reduce the responsibilities and complexity of each
  method residing in child classes.
- Changed getKalturaInternalId to detect entries that are rejected.

## [3.0.1](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-3.0.1) - 2025-07-07

### Changed

- Config to use sessionDurationSeconds and sessionRefreshThreshold instead of keepAliveSeconds.
  KeepAliveSeconds is now calculated from these two parameters and sessionDurationSeconds is used when starting a
  session.
- Changed KeepAliveSession from long to int.
- Changed startWidgetSession to take a nullable Integer to set specific Expiry. This should not be lower than 600
  due to Kaltura caching of responses.

### Added

- Added getSessionInfo that logs sessionInfo. Only used for testing.

## [3.0.0](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-3.0.0) - 2025-06-12

### Changed

- Kaltura AppToken Client changed to use enum types (values) to start session. This was required since Kaltura was
  changed.
- Kaltura upload will validate stream was connected to meta-data and throw IOException if last call (#4 out of 4) is not
  a success
- Extracted methods from uploadMedia to make individual steps/api calls in the uploadprocess more visible and easier
  to maintain.
- startClientSession have been refactored and now only starts a widget session appTokens is used.
- computeHash method no longer set attributes in the kaltura client and only computes the hash value.
- Added API exceptions to upload and other methods using API calls.
- Corrected spelling error in method name getKulturaInternalId to getKalturaInternalId.

### Added

- Method to reject a stream in Kaltura. The stream can not be played with rejected status. A KMC moderator can
  see all rejected videos in the KMC and change status again if needed.
  bumped kb-util to v1.6.8 for consistency.

### Changed

## [1.2.6](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.6) - 2024-11-19

### Added

- Method to reject a stream in Kaltura. The stream can not be played with rejected status. A KMC moderator can
  see all rejected videos in the KMC and change status again if needed.

## [1.2.5](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.5) - 2024-10-15

### Changed

- Fix log typo.
- When client is refreshed, log how many millis since last refresh

## [1.2.4](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.4) - 2024-10-14

### Changed

- Bump kb-util to v1.5.11
- Client can be created with AppTokens and no longer needs kaltura mastersecret. Creating new AppTokens still require
  mastersecret

## [1.2.3](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.3) - 2024-07-18

- Handle rare error from Kaltura with empty response.

## [1.2.2](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.2) - 2024-07-17

- New manuel delete job that takes an input file of kaltura entryId's and deletes each stream+metadata in Kaltura for
  the entries.

## [1.2.1](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.1) - 2024-06-27

### Added

- New method that deletes a stream and all metadata for the stream in Kaltura by the internal Kaltura entryId as key.

### Changed

- Updated kb-util version

## [1.2.0](https://github.com/kb-dk/ds-kaltura/releases/tag/ds-kaltura-1.2.0) - 2024-05-28 - 2024-06-21

- Remove the root `.config` from the configuration
- More methods.

## [1.0.0] - YYYY-MM-DD

### Added

- Initial release of <project>

[Unreleased](https://github.com/kb-dk/ds-kaltura/compare/v1.0.0...HEAD)
[1.0.0](https://github.com/kb-dk/ds-kaltura/releases/tag/v1.0.0)
