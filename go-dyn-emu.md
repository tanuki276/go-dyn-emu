go-dyn-emu

High-Fidelity DynamoDB Emulator Implemented in Go





Overview

go-dyn-emu is a DynamoDB emulator designed to reproduce the behavior of AWS DynamoDB with high precision using only Go.
Its goal is to fully replicate operational logic — including ConditionExpression, UpdateExpression, GSI updates, and transaction behavior — in a local environment.

It is designed not merely as an API-compatible mock, but as a simulator that mirrors the internal behavior and evaluation logic of DynamoDB.
Additionally, it provides a snapshot system that enables fast saving and restoring of database states.





Purpose

1. High-Fidelity Simulation

Implements essential DynamoDB APIs (Put, Get, Query, Scan, Update) in accordance with official specifications

Parses and evaluates ConditionExpression and UpdateExpression, including type handling, operator precedence, and conflict resolution

Performs accurate GSI updates and consistency checks internally


2. Experimental Platform for DBI-Style Data Operation

It provides a mechanism to insert transparent hooks before and after data operations, leveraging Go’s abstraction capabilities and concurrency features.

GSI update logic

Transaction consistency checks

Future extensions for custom logic injection


This enables inspection and analysis of datastore behavior without modifying application-level code.


　


Features

API compatibility with DynamoDB
Supports PutItem, GetItem, Query, Scan, UpdateItem, and other core operations.

Transaction support
Fully implements TransactWriteItems, including isolation and consistency validation.

Complete expression parsing

ConditionExpression

UpdateExpression

ExpressionAttributeNames / Values


Implements full-stack processing (lexing, AST conversion, evaluation) to replicate DynamoDB behavior accurately.

Local persistence via LevelDB
Data persists across process restarts.

Snapshot and restore
Provides fast full-state snapshots and restoration.

Fully isolated local sandbox
Tables and data are completely separated from production environments.




Installation

go mod tidy
go build -o dyn-emu-local





Execution

./dyn-emu-local

Default endpoint: http://localhost:8000





Usage from AWS CLI / SDK

aws configure set dynamodb.endpoint_url http://localhost:8000
aws configure set region us-west-2

Dummy credentials are acceptable.





Examples

Create a table

aws dynamodb create-table \
  --table-name Users \
  --key-schema AttributeName=UserID,KeyType=HASH AttributeName=Timestamp,KeyType=RANGE \
  --attribute-definitions AttributeName=UserID,AttributeType=S AttributeName=Timestamp,AttributeType=N \
  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
  --endpoint-url http://localhost:8000

Put / Get

aws dynamodb put-item ...
aws dynamodb get-item ...

Conditional Update

aws dynamodb update-item ...





Custom APIs

Operation	Target	Description

Create Snapshot	DynamoDB_20120810.CreateSnapshot	Save full DB state
Load Snapshot	DynamoDB_20120810.LoadSnapshot	Restore a saved state
Delete All Data	DynamoDB_20120810.DeleteAllData	Reset underlying LevelDB store





Current Limitations

Category	Description

Not Implemented	TransactGetItems
Not Implemented	Deep Map/List UpdateExpression
Bottleneck	Snapshot creation uses physical copying, slow for large DB
Bottleneck	Scan uses full iteration
Weakness	Error message formats not fully aligned with DynamoDB
　　



Future Work

Copy-on-Write snapshot mechanism

Improved accuracy of error messages

Public API for custom hooks