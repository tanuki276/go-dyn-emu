# go-dyn-emu

**High-Fidelity DynamoDB Emulator Implemented in Go**

## Overview

`go-dyn-emu` is a DynamoDB emulator written entirely in Go, designed to reproduce AWS DynamoDB's behavior with maximum precision.  
It is not just an API-compatible mock — it is a true simulator that replicates DynamoDB's internal evaluation logic, including ConditionExpression, UpdateExpression, GSI updates, and transaction semantics.

It also provides a fast snapshot/restore system for saving and loading entire database states in seconds.

## Purpose

1. **High-Fidelity Simulation**
   - Implements core DynamoDB APIs (PutItem, GetItem, Query, Scan, UpdateItem, etc.) exactly according to official specifications
   - Full parsing and evaluation of ConditionExpression and UpdateExpression (type handling, operator precedence, conflict resolution)
   - Accurate internal GSI updates and consistency checks

2. **Experimental Platform for DBI-Style Data Operations**
   - Allows transparent hook insertion before/after any data operation using Go’s abstraction and concurrency features
   - Useful for inspecting GSI update logic, transaction consistency, or injecting custom behavior in the future
   - Enables deep analysis of datastore behavior without modifying application code

## Features

| Feature                          | Description                                                                 |
|----------------------------------|-----------------------------------------------------------------------------|
| DynamoDB API Compatibility       | Supports PutItem, GetItem, Query, Scan, UpdateItem, and other core operations |
| Full Transaction Support         | Complete implementation of TransactWriteItems with isolation and consistency validation |
| Complete Expression Engine       | ConditionExpression, UpdateExpression, ExpressionAttributeNames/Values<br>Full lexer → AST → evaluator pipeline |
| Local Persistence                | Powered by LevelDB — data survives process restarts                         |
| Snapshot & Restore               | Fast full-database snapshot save/load                                       |
| Fully Isolated Local Sandbox     | Tables and data are completely separated from production                    |

## Installation

```bash
go mod tidy
go build -o dyn-emu-local
```

## Running

```bash
./dyn-emu-local
```

**Default endpoint**: `http://localhost:8000`

## Usage with AWS CLI / SDK

```bash
aws configure set dynamodb.endpoint_url http://localhost:8000
aws configure set region us-west-2
```

Any dummy credentials work (no real AWS auth required).

## Examples

### Create a Table

```bash
aws dynamodb create-table \
  --table-name Users \
  --key-schema AttributeName=UserID,KeyType=HASH AttributeName=Timestamp,KeyType=RANGE \
  --attribute-definitions AttributeName=UserID,AttributeType=S AttributeName=Timestamp,AttributeType=N \
  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
  --endpoint-url http://localhost:8000
```

### Put / Get / Conditional Update

```bash
aws dynamodb put-item ...
aws dynamodb get-item ...
aws dynamodb update-item ...
```

## Custom APIs (Non-standard)

| Operation           | API Name                              | Description                       |
|---------------------|---------------------------------------|-----------------------------------|
| Create Snapshot     | `DynamoDB_20120810.CreateSnapshot`    | Save entire DB state              |
| Load Snapshot       | `DynamoDB_20120810.LoadSnapshot`      | Restore a previously saved state  |
| Delete All Data     | `DynamoDB_20120810.DeleteAllData`     | Wipe the underlying LevelDB store |

## Current Limitations

| Category          | Description                                              |
|-------------------|----------------------------------------------------------|
| Not Implemented   | TransactGetItems                                         |
| Not Implemented   | Deeply nested Map/List operations in UpdateExpression   |
| Performance       | Snapshot creation uses physical file copy (slow for large datasets) |
| Performance       | Scan performs full table iteration                       |
| Minor             | Error message wording/format not 100% identical to real DynamoDB |

## Future Work

- Copy-on-Write snapshots for near-instant save/restore
- Improved error message fidelity
- Public hook API for custom logic injection
- Additional DynamoDB features and refinements

---

**go-dyn-emu** — A high-fidelity, local DynamoDB emulator that behaves as close as possible to the real thing, entirely in Go.
```