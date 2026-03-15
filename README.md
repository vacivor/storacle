# Storacle

`storacle` is a modular Java object storage abstraction with pluggable vendor
clients, a higher-level upload template, Spring Boot auto-configuration, and
built-in support for content-type detection, upload policies, and checksums.

## Features

- Multi-vendor object storage client abstraction
- Higher-level `ObjectStorageTemplate` for upload/download workflows
- Spring Boot starter for fast integration
- Configurable content type detection:
  - `default`
  - `tika`
- Scene-based content policy routing
- Reusable upload policies:
  - `AllowlistContentTypePolicy`
  - `MaxContentLengthPolicy`
  - `CompositeContentTypePolicy`
- Upload checksum support:
  - `SHA-256`
  - `SHA-384`
  - `SHA-512`
  - `MD5`
  - `CRC32`
  - `CRC32C`

## Modules

- `storacle-api`
  - core contracts, value objects, vendor registry, and shared exceptions
- `storacle-core`
  - `ObjectStorageTemplate`, content type detectors, checksum utilities, and upload policies
- `storacle-spring-boot-starter`
  - Spring Boot auto-configuration
- `storacle-spring-boot-example`
  - runnable example application using MinIO
- `storacle-vendor-*`
  - vendor-specific storage client implementations

## Quick Start

Run tests:

```bash
./gradlew test
```

Run the Spring Boot example:

```bash
./gradlew :storacle-spring-boot-example:bootRun
```

See the example API guide here:

- `storacle-spring-boot-example/README.md`

## Content Detection

The Spring Boot starter supports:

```yaml
storacle:
  content-type-detector: default
```

or:

```yaml
storacle:
  content-type-detector: tika
```

Guidance:

- `default`
  - lighter dependency footprint
  - suitable when conservative detection is enough
- `tika`
  - stronger content detection
  - better fit when content-type policy validation matters

## Upload Policies

`ObjectStorageTemplate` supports scene-aware content policy routing via:

- `UploadRequest.scene`
- `ContentTypePolicy`
- `ContentTypePolicyResolver`
- `RoutingContentTypePolicyResolver`

Typical composition:

```java
ContentTypePolicy avatarPolicy = new CompositeContentTypePolicy(List.of(
        new AllowlistContentTypePolicy(Set.of("image/png", "image/jpeg")),
        new MaxContentLengthPolicy(2 * 1024 * 1024)
));

ContentTypePolicyResolver resolver = new RoutingContentTypePolicyResolver(
        Map.of("avatar", avatarPolicy),
        new NoopContentTypePolicy()
);
```

Policy violations throw `StoragePolicyViolationException`.

## Checksums

Template uploads can calculate checksums during streaming upload and expose them
through `ObjectWriteResult`.

Examples:

```java
String sha256 = result.checksum(ChecksumAlgorithm.SHA_256);
String crc32 = result.checksumOrNull(ChecksumAlgorithm.CRC32);
ObjectMetadata metadata = result.metadata();
```

## Licensing

- Project license: MIT
- Third-party notices:
  - `THIRD_PARTY_NOTICES.md`
