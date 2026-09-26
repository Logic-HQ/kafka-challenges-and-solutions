# To simulate a native dual-write event execution by adding a row to your outbox table.

docker exec -it $(docker ps -q -f name=mysql-primary) mysql -u root -proot_secure_password bank_services -e "
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) 
VALUES ('tx_998877_xyz', 'account', 'acc_5511', 'FundsTransferred', '{\"amount\": 1250.00}');"
