# Third-Party Notices

This project depends on third-party open-source software. The following list
covers the additional runtime dependencies introduced for content detection and
related functionality in this repository state.

## Apache Tika

- Component: `org.apache.tika:tika-core`
- Version: `3.2.3`
- License: Apache License 2.0
- Project: <https://tika.apache.org/>

## Apache Commons IO

- Component: `commons-io:commons-io`
- Version: transitively resolved with Apache Tika
- License: Apache License 2.0
- Project: <https://commons.apache.org/proper/commons-io/>

## SLF4J

- Component: `org.slf4j:slf4j-api`
- Version: transitively resolved with Apache Tika
- License: MIT License
- Project: <https://www.slf4j.org/>

## Notes

- This file is a lightweight notice summary for the dependencies added during
  the content detection work.
- When distributing binaries, include the corresponding license texts and any
  required NOTICE information for Apache-licensed dependencies.
