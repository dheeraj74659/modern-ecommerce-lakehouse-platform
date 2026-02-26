# Modern E-Commerce Lakehouse Platform

## Overview

This project implements a production-grade batch data lakehouse platform using a Medallion Architecture (Bronze, Silver, Gold). The platform processes high-volume e-commerce transactional data with incremental ingestion, SCD Type 2 handling, idempotent transformations, and orchestrated workflows.

The system is designed to support scalable analytics workloads while maintaining data quality, historical integrity, and cost-efficient storage.

---

## Architecture Principles

- Incremental, idempotent processing
- ACID-compliant storage using Delta Lake
- Separation of raw, refined, and curated layers
- Schema enforcement and evolution handling
- Orchestrated DAG-based workflow execution
- Data quality validation before promotion to higher layers
- Optimized partitioning and file compaction strategies

---

## Technology Stack

- Apache Spark (Batch Processing)
- Delta Lake (Storage Layer)
- Amazon S3 (Object Storage)
- Apache Airflow (Orchestration)
- Amazon Redshift (Analytics Serving Layer)

---

## Data Volume

- ~40 GB raw data ingestion daily
- 6-hour batch cycles
- ~150M records processed per day
- Optimized Gold layer for BI consumption (~8 GB/day)

---

## Medallion Architecture

### Bronze Layer
- Append-only ingestion
- Raw data preservation
- Schema enforcement
- Ingestion metadata tracking

### Silver Layer
- Deduplication
- Type casting & normalization
- Business validation rules
- Incremental upsert logic

### Gold Layer
- Star schema modeling
- Fact and dimension tables
- SCD Type 2 implementation
- Aggregation and KPI-ready datasets

---

## Key Engineering Features

- Delta MERGE-based incremental loads
- SCD Type 2 customer dimension
- Config-driven pipeline execution
- Partition pruning optimization
- Z-Ordering for query performance
- Data validation checks prior to table promotion
- Airflow DAG orchestration with task dependency management

---

## Performance Optimization Strategy

- Adaptive query execution enabled
- Optimized shuffle partitions
- Partitioned storage by date
- Scheduled OPTIMIZE and VACUUM jobs
- File compaction to prevent small-file problem

---

## Fault Tolerance & Reliability

- Idempotent pipeline runs
- Checkpoint tracking for incremental loads
- Schema evolution support
- Late-arriving data handling strategy

---

## Scalability Strategy

The system can scale horizontally by:
- Increasing Spark executor parallelism
- Optimizing Delta partitioning
- Expanding Redshift compute nodes
- Parallelizing Airflow DAG execution

---

## Future Enhancements

- CDC-based ingestion
- Data lineage integration
- Automated anomaly detection
- Real-time + batch hybrid architecture
