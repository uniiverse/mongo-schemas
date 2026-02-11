# Project Specification: Real-Time Kafka MongoDB Schema Discovery Service

**Version**: 0.1
**Date**: 2026-02-11
**Status**: Draft - Awaiting Feedback

---

## 1. Overview

**Service Name**: `schema-discovery-service`

**Purpose**: Continuously consume MongoDB collection data from Kafka topics (`uni.bronze.web.mongodb.*`) and maintain up-to-date schema definitions for each collection.

**Background**: Legacy MongoDB database with 10 years of data where collections have evolved organically, resulting in fields with inconsistent or multiple data types across documents. Data is replicated to Kafka topics for downstream processing.

### 1.1 System Context & Value Proposition

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS                                         │
│                                                                                     │
│   ┌──────────────────────────────────────────────────────────┐                      │
│   │           Legacy MongoDB (10+ years)                     │                      │
│   │                                                          │                      │
│   │  users         orders        products       sessions     │                      │
│   │  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐  │                      │
│   │  │ age: int │  │ total:  │  │ price:   │  │ duration:│  │                      │
│   │  │ age: str │  │  double │  │  string  │  │  int32   │  │                      │
│   │  │ age: null│  │ total:  │  │ price:   │  │ duration:│  │                      │
│   │  │  (mixed) │  │  string │  │  double  │  │  string  │  │                      │
│   │  └─────────┘  └─────────┘  └──────────┘  └──────────┘  │                      │
│   └──────────────────────┬───────────────────────────────────┘                      │
│                          │ CDC / Replication                                         │
│                          ▼                                                           │
│   ┌──────────────────────────────────────────────────────────┐                      │
│   │                    Apache Kafka                          │                      │
│   │                                                          │                      │
│   │  uni.bronze.web.mongodb.users                            │                      │
│   │  uni.bronze.web.mongodb.orders                           │                      │
│   │  uni.bronze.web.mongodb.products                         │                      │
│   │  uni.bronze.web.mongodb.sessions                         │                      │
│   │  uni.bronze.web.mongodb.*                                │                      │
│   └──────────────────────┬───────────────────────────────────┘                      │
└──────────────────────────┼──────────────────────────────────────────────────────────┘
                           │
                           │ Continuous consumption
                           ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                     SCHEMA DISCOVERY SERVICE                                         │
│                                                                                      │
│   ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────────────────────┐  │
│   │ Topic Discovery  │──▶│ Schema Extractor │──▶│ Schema Merger                   │  │
│   │                  │   │                  │   │                                 │  │
│   │ Watches for new  │   │ Parses each msg, │   │ Merges newly observed types     │  │
│   │ collection topics│   │ extracts all     │   │ into existing schema per        │  │
│   │ dynamically      │   │ field:type pairs │   │ collection, persists to Redis   │  │
│   └─────────────────┘   └──────────────────┘   └───────────────┬─────────────────┘  │
│                                                                 │                    │
│                                                                 ▼                    │
│                                                  ┌──────────────────────────────┐    │
│                                                  │ Redis (Schema Store)         │    │
│                                                  │                              │    │
│                                                  │ schema:users                 │    │
│                                                  │   id: [string]               │    │
│                                                  │   age: [int32, string, null] │    │
│                                                  │   email: [string]            │    │
│                                                  │                              │    │
│                                                  │ schema:orders                │    │
│                                                  │   total: [double, string]    │    │
│                                                  │   ...                        │    │
│                                                  └──────────────┬───────────────┘    │
│                                                                 │                    │
│                                                                 ▼                    │
│                                                  ┌──────────────────────────────┐    │
│                                                  │ REST API                     │    │
│                                                  │                              │    │
│                                                  │ GET /schemas                 │    │
│                                                  │ GET /schemas/{collection}    │    │
│                                                  │ GET /health                  │    │
│                                                  └──────────────┬───────────────┘    │
│                                                                 │                    │
└─────────────────────────────────────────────────────────────────┼────────────────────┘
                                                                  │
                      ┌───────────────────────────────────────────┼──────────────┐
                      │              DOWNSTREAM CONSUMERS         │              │
                      │                                           │              │
          ┌───────────┼──────────┬────────────────────────────────┘              │
          │           │          │                                               │
          ▼           ▼          ▼                                               │
┌─────────────┐ ┌──────────┐ ┌───────────────────┐                              │
│    DATA     │ │   DATA   │ │   ETL PIPELINE    │                              │
│  CLEANING   │ │ CATALOG  │ │   GENERATION      │                              │
│             │ │          │ │                   │                              │
│ Know which  │ │ Auto-    │ │ Generate type-    │                              │
│ fields have │ │ populate │ │ safe transforms   │                              │
│ mixed types │ │ field    │ │ and casts based   │                              │
│ and need    │ │ metadata │ │ on actual source  │                              │
│ coercion or │ │ with     │ │ data types.       │                              │
│ validation  │ │ accurate │ │                   │                              │
│ rules.      │ │ source   │ │ Example:          │                              │
│             │ │ schemas. │ │ age: [int32, str] │                              │
│ Example:    │ │          │ │ → CAST(age AS INT)│                              │
│ age has     │ │ No more  │ │   with fallback   │                              │
│ [int32,     │ │ manual   │ │   handling for    │                              │
│  string,    │ │ schema   │ │   string values   │                              │
│  null]      │ │ entry.   │ │                   │                              │
│ → define    │ │ Always   │ │ Generates         │                              │
│   cleanup   │ │ reflects │ │ pipeline configs  │                              │
│   rules to  │ │ actual   │ │ that account for  │                              │
│   normalize │ │ data in  │ │ every type a      │                              │
│   to int32  │ │ prod.    │ │ field can be.     │                              │
└─────────────┘ └──────────┘ └───────────────────┘                              │
                                                                                │
│  THE PROBLEM THIS SOLVES:                                                     │
│                                                                               │
│  Without this service, teams must manually inspect collections to discover    │
│  field types — an impossible task across 10 years of schema drift.            │
│  ETL pipelines break on unexpected types, data catalogs are stale, and       │
│  cleaning rules are written reactively after failures.                        │
│                                                                               │
│  With this service, the actual source-of-truth schema is always available,   │
│  continuously updated, and machine-readable — enabling automated data        │
│  quality, cataloging, and pipeline generation.                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Key Value Drivers

| Consumer | Problem Without This Service | Value With This Service |
|----------|------------------------------|------------------------|
| **Data Cleaning** | Teams discover mixed types reactively when pipelines break. Cleaning rules are incomplete and lag behind reality. | Proactively identifies every field with mixed types. Cleaning rules can be generated and validated against the live schema. |
| **Data Cataloging** | Schema metadata is manually entered, quickly becomes stale, and doesn't reflect actual data in production. | Auto-populates catalog entries with accurate, continuously-updated field-level type information directly from production data. |
| **ETL Pipeline Generation** | Pipelines assume field types and fail on edge cases (e.g., `age` is usually `int32` but sometimes `string`). | Pipelines are generated with full type awareness — casting logic, null handling, and fallback paths are informed by the real type distribution per field. |

---

## 2. Functional Requirements

### 2.1 Core Functionality

| Requirement | Description |
|-------------|-------------|
| **Topic Discovery** | Auto-discover all Kafka topics matching pattern `uni.bronze.web.mongodb.*` |
| **Dynamic Subscription** | Subscribe to new topics as they appear without service restart |
| **Schema Extraction** | Parse each message and extract field names and data types |
| **Schema Merging** | Update existing schema with newly discovered field types |
| **Schema Storage** | Persist one schema record per collection in Redis |
| **API Access** | Expose REST API to query schemas |

### 2.2 Schema Format

**Redis Key Pattern**: `schema:{collection_name}`

**Value Structure** (JSON):
```json
{
  "_id": "users",
  "collection_name": "users",
  "topic": "uni.bronze.web.mongodb.users",
  "schema": {
    "id": ["string"],
    "email": ["string", "null"],
    "age": ["number", "string"],
    "created_at": ["date", "string"],
    "address.city": ["string"],
    "address.zip": ["string", "number"],
    "tags": ["array"]
  },
  "last_updated": "2026-02-11T10:30:00Z",
  "documents_processed": 1500000
}
```

### 2.3 Type Detection Rules

The following covers all BSON types defined in the MongoDB specification:

| BSON Type ID | MongoDB Type | Detected As | Notes |
|:---:|---|---|---|
| 1 | Double | `"double"` | 64-bit floating point |
| 2 | String | `"string"` | UTF-8 encoded |
| 3 | Object | `"object"` | Embedded document (also flattened via dot notation) |
| 4 | Array | `"array"` | Ordered list |
| 5 | Binary Data | `"bindata"` | Binary with subtype (generic, UUID, MD5, encrypted, etc.) |
| 7 | ObjectId | `"objectid"` | 12-byte unique identifier |
| 8 | Boolean | `"boolean"` | True/false |
| 9 | Date | `"date"` | UTC datetime (ms since epoch) |
| 10 | Null | `"null"` | Null value |
| 11 | Regular Expression | `"regex"` | Pattern + options string |
| 13 | JavaScript | `"javascript"` | JS code without scope |
| 16 | Int32 | `"int32"` | 32-bit signed integer |
| 17 | Timestamp | `"timestamp"` | Internal MongoDB replication/sharding type |
| 18 | Int64 | `"int64"` | 64-bit signed integer |
| 19 | Decimal128 | `"decimal128"` | 128-bit decimal floating point (IEEE 754-2008) |
| -1 | MinKey | `"minkey"` | Lowest possible BSON comparison value |
| 127 | MaxKey | `"maxkey"` | Highest possible BSON comparison value |

**Deprecated BSON types** (detected if encountered in legacy data):

| BSON Type ID | MongoDB Type | Detected As | Notes |
|:---:|---|---|---|
| 6 | Undefined | `"undefined"` | Deprecated since MongoDB 4.0 |
| 12 | DBPointer | `"dbpointer"` | Deprecated database reference |
| 14 | Symbol | `"symbol"` | Deprecated symbol type |
| 15 | JavaScript with Scope | `"javascript_ws"` | Deprecated since MongoDB 4.4 |

> **Design decision**: Unlike the previous draft which collapsed `int32`, `int64`, and `double` into a single `"number"` type, this version preserves the specific numeric type. This is important for a 10-year-old database where fields may have migrated between `int32` → `int64` or `double` → `decimal128` over time, and downstream consumers need to know the exact types present.

### 2.4 Nested Field Handling

- Flatten nested objects using dot notation
- Example: `{"user": {"address": {"city": "NYC"}}}` → `"user.address.city": ["string"]`
- Arrays: detect as `"array"` type (optional: detect element types as `"array[string]"`)

---

## 3. Technical Architecture

### 3.1 Components

```
schema-discovery-service/
├── src/
│   ├── main.py                    # Entry point
│   ├── config.py                  # Configuration management
│   ├── kafka/
│   │   ├── consumer.py            # Kafka consumer wrapper
│   │   ├── topic_discovery.py    # Topic pattern discovery
│   │   └── message_processor.py  # Message parsing
│   ├── schema/
│   │   ├── extractor.py           # Extract schema from document
│   │   ├── merger.py              # Merge new types into existing schema
│   │   └── storage.py             # Redis schema persistence
│   ├── api/
│   │   ├── app.py                 # FastAPI application
│   │   └── routes.py              # API endpoints
│   └── utils/
│       ├── logger.py              # Logging configuration
│       └── metrics.py             # Prometheus metrics (optional)
├── tests/
│   ├── test_extractor.py
│   ├── test_merger.py
│   └── test_api.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

### 3.2 Technology Stack

| Component | Technology | Justification |
|-----------|-----------|---------------|
| **Language** | Python 3.11+ | Rich Kafka/MongoDB ecosystem |
| **Kafka Client** | `confluent-kafka-python` | High performance, auto topic discovery |
| **Redis Client** | `redis-py` | Official driver, async support |
| **API Framework** | FastAPI | Async, auto-docs, modern |
| **Schema Storage** | Redis | See Section 3.4 |
| **Containerization** | Docker | Portability, easy deployment |

### 3.4 Schema Storage: Redis

Given the access pattern (single record per collection, low reads, writes heavy only during initial ingestion, must serve other services), Redis is the right fit.

| Aspect | Details |
|--------|---------|
| **Data Model** | One key per collection: `schema:{collection_name}` → JSON blob |
| **K8s Deployment** | Single replica via Bitnami Helm chart |
| **Persistence** | RDB snapshots + AOF for durability |
| **Resource Footprint** | ~50-128MB RAM for this workload |
| **Why Redis** | Simple key-value semantics, sub-ms reads for API consumers, mature K8s Helm charts, accessible from multiple services |

Example data model:
```
SET schema:users '{"schema": {"id": ["string"], "age": ["int32", "string"]}, ...}'
SET schema:orders '{"schema": {"total": ["double", "decimal128"]}, ...}'
GET schema:users  → full schema JSON
KEYS schema:*     → list all collections
```

### 3.3 Data Flow

```
1. Topic Discovery Thread (every 60s)
   └─> Admin API lists topics matching uni.bronze.web.mongodb.*
   └─> Creates consumer for new topics

2. Per-Topic Consumer Thread
   └─> Reads message from Kafka
   └─> Parses JSON document
   └─> Extracts field:type mappings (recursive for nested)
   └─> Sends to Schema Merger queue

3. Schema Merger Thread (batched every 10s or 100 messages)
   └─> Fetches current schema from Redis (GET schema:{collection})
   └─> Merges new types (union of arrays)
   └─> Writes back to Redis (SET schema:{collection})
   └─> Increments documents_processed counter

4. REST API (FastAPI)
   └─> Serves schema queries on-demand
```

---

## 4. API Specification

### 4.1 Endpoints

#### GET `/schemas`
List all collection schemas

**Response**:
```json
{
  "schemas": [
    {
      "collection_name": "users",
      "last_updated": "2026-02-11T10:30:00Z",
      "field_count": 15,
      "documents_processed": 1500000
    }
  ]
}
```

#### GET `/schemas/{collection_name}`
Get detailed schema for a collection

**Response**:
```json
{
  "collection_name": "users",
  "topic": "uni.bronze.web.mongodb.users",
  "schema": {
    "id": ["string"],
    "email": ["string", "null"],
    "age": ["number", "string"]
  },
  "last_updated": "2026-02-11T10:30:00Z",
  "documents_processed": 1500000
}
```

#### GET `/health`
Service health check

**Response**:
```json
{
  "status": "healthy",
  "kafka_connected": true,
  "redis_connected": true,
  "active_consumers": 25
}
```

---

## 5. Configuration

**Environment Variables** (`.env`):

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_PATTERN=uni.bronze.web.mongodb.*
KAFKA_GROUP_ID=schema-discovery-service
KAFKA_AUTO_OFFSET_RESET=latest

# Redis (Schema Storage)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_KEY_PREFIX=schema:

# Service
TOPIC_DISCOVERY_INTERVAL_SEC=60
SCHEMA_MERGE_BATCH_SIZE=100
SCHEMA_MERGE_INTERVAL_SEC=10
LOG_LEVEL=INFO

# API
API_HOST=0.0.0.0
API_PORT=8000
```

---

## 6. Implementation Plan

### Phase 1: Core Schema Extraction (Week 1)
- [ ] Project setup (repo, dependencies, Docker)
- [ ] Implement `schema/extractor.py` (document → field:type mapping)
- [ ] Implement `schema/merger.py` (merge logic)
- [ ] Unit tests for extraction and merging
- [ ] Redis storage layer

### Phase 2: Kafka Integration (Week 1-2)
- [ ] Kafka consumer wrapper with error handling
- [ ] Topic discovery using Admin API
- [ ] Dynamic consumer creation per topic
- [ ] Message processing pipeline
- [ ] Integration tests with test Kafka cluster

### Phase 3: API & Deployment (Week 2)
- [ ] FastAPI application with endpoints
- [ ] Health check endpoint
- [ ] Dockerfile and docker-compose setup
- [ ] Documentation (README, API docs)
- [ ] End-to-end testing

### Phase 4: Production Readiness (Week 3)
- [ ] Logging and monitoring (structured logs)
- [ ] Graceful shutdown handling
- [ ] Consumer lag monitoring
- [ ] Performance testing with high-volume topics
- [ ] Deployment documentation

---

## 7. Non-Functional Requirements

| Requirement | Target |
|-------------|--------|
| **Throughput** | Process 10K+ messages/sec across all topics |
| **Latency** | Schema updates within 10 seconds of new type discovery |
| **Availability** | 99.5% uptime (can tolerate brief restarts) |
| **Scalability** | Horizontal scaling via multiple consumer instances |
| **Resource Usage** | < 2GB RAM per instance under normal load |

---

## 8. Open Questions / Future Enhancements

- **Array element types**: Track types inside arrays? (`array[string]` vs just `array`)
- **Sampling**: For very large collections, sample-based analysis to reduce load?
- **Alerts**: Notify when conflicting types discovered (e.g., field switches from `number` to `string`)?
- **Dead letter queue**: For malformed messages that can't be parsed?
---

## 9. Assumptions

1. Kafka messages contain full MongoDB documents as JSON in the message value
2. Message format is consistent across all topics
3. Schema storage uses a dedicated Redis instance
4. No message ordering guarantees required
5. Schema updates can be eventually consistent (10-60s delay acceptable)
6. No schema versioning or history tracking needed
7. Service can start from `latest` offset (doesn't need to process historical data on startup)

---

## 10. Success Criteria

- [ ] Service successfully discovers and subscribes to all matching Kafka topics
- [ ] Schemas accurately reflect all data types present in collections
- [ ] API responds to schema queries within 100ms
- [ ] Service handles 10K+ messages/sec without falling behind
- [ ] Zero data loss during normal operation
- [ ] Service recovers gracefully from Kafka/Redis outages

---

## Notes for Review

Please mark up this document with:
- ✅ Sections that look good
- ❌ Things that need to change
- ❓ Areas that need clarification
- 💡 Suggestions or additions

Any structural changes, missing requirements, or incorrect assumptions - let me know!
