#!/usr/bin/env bash
set -euo pipefail

# 1. Configurations
REGISTRY_URL="http://localhost:8081"
SUBJECT="mysql_cluster.bank_services.outbox_events-value"
SCHEMA_FILE="./schemas/outbox_event.avsc"

echo "=== Starting Schema Validation and Migration ==="

# Minify JSON schema to safely pass it over the curl payload
RAW_SCHEMA=$(jq -c . "$SCHEMA_FILE")
# Escape quotes for valid JSON encapsulation
ESCAPED_SCHEMA=$(echo "$RAW_SCHEMA" | sed 's/"/\\"/g')
PAYLOAD="{\"schema\": \"$ESCAPED_SCHEMA\"}"

# 2. Check Schema Compatibility Before Deploying
echo "Verifying schema compatibility against history..."
COMPAT_CHECK=$(curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$PAYLOAD" \
  "$REGISTRY_URL}/compatibility/subjects/${SUBJECT}/versions/latest" || echo '{"is_compatible":false}')

IS_COMPATIBLE=$(echo "$COMPAT_CHECK" | jq -r '.is_compatible // false')

if [ "$IS_COMPATIBLE" != "true" ]; then
  echo "❌ CRITICAL: The updated schema violates the registry's compatibility guidelines!"
  echo "Registry Response: $COMPAT_CHECK"
  exit 1
fi

echo "✅ Schema passes compatibility validation tests successfully."

# 3. Register the Evolved Schema
echo "Registering new schema version..."
REG_RESPONSE=$(curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$PAYLOAD" \
  "$REGISTRY_URL/subjects/${SUBJECT}/versions")

SCHEMA_ID=$(echo "$REG_RESPONSE" | jq -r '.id // empty')

if [ -z "$SCHEMA_ID" ]; then
  echo "❌ CRITICAL: Schema registration failed!"
  echo "Registry Response: $REG_RESPONSE"
  exit 1
fi

echo "🚀 Success! Schema version deployed and verified under Global Schema ID: $SCHEMA_ID"
