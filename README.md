# fndd

A professional Python library and toolkit for fast news data detection — a modular, extensible system for ingesting, processing, and analysing news data streams with a focus on reliability, observability, and clean engineering.

https://img.shields.io/badge/Python-3.9%2B-blue
https://img.shields.io/badge/License-MIT-green
https://img.shields.io/badge/Status-Active-brightgreen

Table of Contents

Overview

Key Features

Architecture

Installation

Quick Start

Usage

Configuration

Project Structure

API Reference

Testing

Roadmap

Contributing

License

Overview

fndd is a Python package that provides building blocks for working with news data. It offers a clean pipeline for fetching, validating, normalising, and analysing articles, and it is designed to be embedded in larger data-processing systems or used standalone via a command-line interface.

The project emphasises:

Correctness — defensive parsing and validation everywhere.

Observability — structured logging and clear error reporting.

Extensibility — pluggable sources, analysers, and exporters.

Testability — pure functions and dependency injection.

Key Features

Pluggable data sources — define a source once, register it, reuse it.

Immutable domain models — dataclasses with type hints.

In-memory pipeline — a simple, transparent processing chain.

Structured logging — via the standard logging module.

Text analysis — keyword extraction, reading time, and sentiment heuristics.

JSON export — persist processed records easily.

Command-line interface — run the pipeline without writing code.

Zero mandatory third-party dependencies — standard library only by default.

Architecture

The library is organised around a small set of composable primitives:

Sources produce raw news items.

Parsers validate and normalise raw items into domain objects.

Analysers enrich each item with derived attributes.

Exporters persist processed items.

Each stage is an independent function, which makes the pipeline easy to reason about and extend.

Installation

```bash
git clone https://github.com/Shiv-0707/fndd.git
cd fndd
python -m venv .venv
source .venv/bin/activate # Windows: .venv\Scripts\activate
```

The core library requires only Python 3.9+. Optional tooling can be installed with:

```bash
pip install -r requirements-dev.txt
```

Quick Start

```bash
python main.py --help
python main.py run
```

Usage

Run the built-in demo pipeline:

```bash
python main.py run
```

Export results to JSON:

```bash
python main.py run --export output.json
```

Enable verbose logging:

```bash
python main.py run --verbose
```

Configuration
Flag  Description  Default
--verbose  Enable debug logging  off
--export  Path to write the JSON export  none
--limit  Maximum number of items to process  0 (all)
Project Structure

```
fndd/
└── main.py
```

The single-module layout keeps the project easy to read while still demonstrating a full pipeline.

API Reference
`NewsItem`

Immutable value object representing a single news record.

`analyse_item(item)`

Enrich a NewsItem with derived analytics.

`run_pipeline(...)`

Execute the full fetch → parse → analyse pipeline.

`export_json(records, path)`

Persist processed records to disk as JSON.

Testing

```bash
python -m unittest discover
```

Roadmap
□ 

Add HTTP source adapters

□ 

Add persistent storage backends

□ 

Add pluggable sentiment models

□ 

Add CI workflow

Contributing

Contributions are welcome. Please open an issue to discuss significant changes before submitting a pull request.

License

This project is licensed under the MIT License.
