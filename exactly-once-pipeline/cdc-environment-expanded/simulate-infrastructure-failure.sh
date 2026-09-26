# in place, you can simulate a production infrastructure failure by forcing a hard shutdown on one of the nodes

docker-compose stop Debezium-node-1

docker-compose-distributed-workers stop Debezium-node-1
