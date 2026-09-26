-- Verify End-to-End Exactly-Once Data Ingestion

INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) 
VALUES ('evt_abc123_xyz', 'account', 'acc_8899', 'MoneyWithdrawn', '{"amount": 250.00}');
