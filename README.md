Prometheus Parquet Library (WIP)

Status: 🚧 Very early stage – expect breaking changes and rapid iteration

This project aims to provide a shared Go library for working with Parquet-encoded time series data across Prometheus-related projects such as Cortex and Thanos.
📌 Goal

The core objective is to define a common Parquet schema and implement encoding/decoding logic that can be reused to:

    Enable more efficient and scalable long-term storage in object stores like S3, GCS, and Azure Blob.

    Store and query time series data in a Parquet format.

    Reduce duplication across projects that are independently experimenting with Parquet-based storage.

🔬 Current Status

This repository is in a very early phase. We're still experimenting with the schema design, low-level encoding strategies, and how this can plug into Cortex, Thanos, or other Prometheus-compatible systems.

Expect:

    Rapid changes in structure and API

    Incomplete or unstable features

    Minimal documentation

📦 Planned Features

    Reusable Go types for time series + metadata

    Parquet schema definitions (with logical type hints for efficiency)

    High-performance encoders/decoders

    Utilities for block-level indexing, filtering, and compression

    Test data generators for benchmarking

🤝 Contributions

Ideas, feedback, and code contributions are welcome — but please note that the design is still in flux. If you're interested in contributing, feel free to open an issue or discussion.
📄 License