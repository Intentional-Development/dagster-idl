# Changelog

All notable changes to dagster-idl will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- (Placeholder for in-progress work)

## [0.9.3] - 2025-01-03

- **dagster-idl:** version alignment, no functional changes.

## [0.9.2] - 2026-05-02

### Added (Wave 15 — Initial Release)
- **Dagster asset extraction:** 11 nodes, 9 edges (8 single assets + 1 multi_asset with 2 outputs)
- **DQ1 validation:** Parametric variance corpus (cardinality patterns: 1→1, N→1, N→N, 1→N)
- **DQ5 validation:** Fan strategy corpus (broadcast, partition patterns)
- **Patterns covered:** Linear chains, partitioned pipelines, fan-in aggregation, fan-out multi-asset
- **Conformance reports:** DQ1 validation report, DQ5 validation report, conformance summary
- **Extractor:** `extract_dagster.py` (Python, handles @asset, @multi_asset, partitions, dependencies)

### Documentation
- `intent/conformance/conformance.md` — Corpus overview
- `intent/conformance/dq1-validation.md` — Variance patterns validation
- `intent/conformance/dq5-validation.md` — Fan strategy patterns validation

### Notes
- This corpus is a **research/validation corpus** for design questions, not a production extraction target
- Schema compatibility: validated against v0.1.4 (no v0.1.5 features needed for dataflow graphs)
