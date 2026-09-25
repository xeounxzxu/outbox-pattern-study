-- Schema compatible with tomorrow-one/transactional-outbox 4.0.0 (Apache-2.0).
-- https://github.com/tomorrow-one/transactional-outbox
CREATE SEQUENCE outbox_kafka_id_seq;
CREATE TABLE outbox_kafka (
    id BIGINT PRIMARY KEY DEFAULT nextval('outbox_kafka_id_seq'),
    created TIMESTAMP NOT NULL,
    processed TIMESTAMP,
    topic VARCHAR(128) NOT NULL,
    key VARCHAR(128),
    value BYTEA NOT NULL,
    headers JSONB
);
CREATE INDEX idx_outbox_kafka_not_processed ON outbox_kafka (id) WHERE processed IS NULL;
CREATE INDEX idx_outbox_kafka_processed ON outbox_kafka (processed);
CREATE TABLE outbox_kafka_lock (
    id VARCHAR(32) PRIMARY KEY,
    owner_id VARCHAR(128) NOT NULL,
    valid_until TIMESTAMP NOT NULL
);
