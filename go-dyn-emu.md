go-dyn-emu: A High-Fidelity DynamoDB Emulator in Go


---

🚀 Project Introduction

go-dyn-emu is a DynamoDB-compatible emulator written entirely in Go. It uses LevelDB as its persistent storage engine to reproduce DynamoDB-like behavior, making it a practical tool for local development, unit tests, and integration tests.

Key Features

DynamoDB API Compatibility: Supports core DynamoDB operations.

Transaction Support: Implements TransactWriteItems.

Secondary Indexes: Supports Global Secondary Indexes (GSI).

Expression Handling: Parses and evaluates ConditionExpression and UpdateExpression.

Local Persistence: Durable LevelDB-backed storage.

Snapshot System: Save and restore database states instantly.



---

🛠️ Installation and Setup

Prerequisites

Go 1.20+

Properly initialized Go module


1. Build the Emulator

go mod tidy
go build -o dyn-emu-local

2. Run the Server

The emulator listens on port 8000.

./dyn-emu-local

3. Configure AWS CLI / SDK for Local Use

aws configure set dynamodb.endpoint_url http://localhost:8000
aws configure set region us-west-2

Dummy credentials are sufficient.


---

⚙️ Core Usage Examples

1. Create a Table

aws dynamodb create-table \
  --table-name Users \
  --key-schema AttributeName=UserID,KeyType=HASH AttributeName=Timestamp,KeyType=RANGE \
  --attribute-definitions AttributeName=UserID,AttributeType=S AttributeName=Timestamp,AttributeType=N \
  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
  --endpoint-url http://localhost:8000

2. Put and Get Item

aws dynamodb put-item \
  --table-name Users \
  --item '{"UserID":{"S":"u123"},"Timestamp":{"N":"1678886400"},"Username":{"S":"Alice"}}' \
  --endpoint-url http://localhost:8000

aws dynamodb get-item \
  --table-name Users \
  --key '{"UserID":{"S":"u123"},"Timestamp":{"N":"1678886400"}}' \
  --endpoint-url http://localhost:8000

3. Update with Condition

aws dynamodb update-item \
  --table-name Users \
  --key '{"UserID":{"S":"u123"},"Timestamp":{"N":"1678886400"}}' \
  --update-expression "SET #UN = :newname ADD #C :incr" \
  --condition-expression "attribute_exists(Username)" \
  --expression-attribute-names '{"#UN":"Username","#C":"Count"}' \
  --expression-attribute-values '{":newname":{"S":"Bob"},":incr":{"N":"1"}}' \
  --return-values ALL_NEW \
  --endpoint-url http://localhost:8000


---

💾 Emulator-Specific Commands

These use custom X-Amz-Target headers.

Operation	Target	Description

Create Snapshot	DynamoDB_20120810.CreateSnapshot	Save current LevelDB state to file.
Load Snapshot	DynamoDB_20120810.LoadSnapshot	Restore LevelDB from snapshot.
Delete All Data	DynamoDB_20120810.DeleteAllData	Full wipe including schemas.


Snapshot Example

curl -X POST http://localhost:8000/dynamodb \
  -H 'X-Amz-Target: DynamoDB_20120810.CreateSnapshot' \
  -H 'Content-Type: application/x-amz-json-1.0' \
  -d '{"SnapshotName":"baseline_state"}'

curl -X POST http://localhost:8000/dynamodb \
  -H 'X-Amz-Target: DynamoDB_20120810.LoadSnapshot' \
  -H 'Content-Type: application/x-amz-json-1.0' \
  -d '{"SnapshotName":"baseline_state"}'