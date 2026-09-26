-- Simulate a regular successful write:
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) 
VALUES ('evt_clean_1', 'account', 'acc_1', 'MoneyWithdrawn', '{"amount": 50.00}');

-- Simulate a malformed poison pill payload (invalid JSON format):
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) 
VALUES ('evt_poison_2', 'account', 'acc_2', 'MoneyWithdrawn', '{"amount": invalid_json_here}');
