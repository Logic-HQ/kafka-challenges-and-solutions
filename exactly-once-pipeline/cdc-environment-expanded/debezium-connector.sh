curl -X POST -H "Content-Type: application/json" --data '{
  "name": "mysql-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql-primary",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "root_secure_password",
    "database.server.id": "184054",
    "database.topic.prefix": "mysql_cluster",
    "table.include.list": "bank_services.outbox_events",
    
    /* Use Avro converters for keys and values */
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schema-changes.outbox",
    "producer.enable.idempotence": "true",
    "producer.acks": "all",
    
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field": "aggregate_type",
    "transforms.outbox.id.by.field": "id",
    "transforms.outbox.route.topic.replacement": "${routedByValue}-events"
  }
}' http://localhost:8083/connectors

# To Update the Schema Registry Policy to FULL
curl -X PUT -H "Content-Type: application/json" \
  --data '{"compatibility": "FULL"}' \
  http://localhost:8081/config/mysql_cluster.bank_services.outbox_events-value

# If you are not using a dynamic DNS load balancer, update the running Debezium configuration by sending a PUT request to update the database.hostname field
curl -X PUT -H "Content-Type: application/json" \
  --data '{"database.hostname": "mysql-read-replica-backup.internal.net"}' \
  http://localhost:8083/connectors/mysql-replica-ha-connector/config

