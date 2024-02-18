#####create fide database
CREATE DATABASE IF NOT EXISTS fidedw ON CLUSTER ck_replica3
;

#####create fide_w user account
CREATE USER IF NOT EXISTS fide_w ON CLUSTER ck_replica3
	IDENTIFIED WITH SHA256_HASH BY '****************************************************************'
	DEFAULT ROLE read_write
	DEFAULT DATABASE fidedw
;

#####grant privileges to fide_w user
GRANT ON CLUSTER ck_replica3 SHOW, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP DICTIONARY, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE, dictGet ON fidedw.* TO fide_w


#########################################################################################################################
DROP TABLE IF EXISTS fidedw.ods_app_metrics_ed;

#####create single metrics table type1
CREATE TABLE IF NOT EXISTS fidedw.ods_app_metrics_ed
(
	`metrics_time` DateTime,
    `metrics_name` String,
    `labels` Array(String),
    `value` Float64,
	`updated` DateTime DEFAULT now(),
    INDEX idx_app_metrics_label_set (labels, metrics_name) TYPE set(0) GRANULARITY 4
)
ENGINE = GraphiteMergeTree('app_graphite_rollup')
PARTITION BY toYYYYMMDD(metrics_time)
ORDER BY (metrics_time, metrics_name, labels)
TTL metrics_time + INTERVAL 1 MONTH DELETE
;

#####create single metrics table type2
CREATE TABLE IF NOT EXISTS fidedw.ods_app_metrics_ed
(
	`metrics_time` DateTime CODEC(DoubleDelta, LZ4),
    `metrics_name` LowCardinality(String),
    `labels` Array(LowCardinality(String)),
    `value` Float64 CODEC(Gorilla, LZ4),
	`updated` DateTime DEFAULT now(),
    INDEX idx_app_metrics_label_set (labels, metrics_name) TYPE set(0) GRANULARITY 4
)
ENGINE = GraphiteMergeTree('app_graphite_rollup')
PARTITION BY toYYYYMMDD(metrics_time)
ORDER BY (metrics_time, metrics_name, labels)
TTL metrics_time + INTERVAL 1 MONTH DELETE
;

#########################################################################################################################

DROP TABLE IF EXISTS fidedw.ods_app_metrics_ed ON CLUSTER ck_replica3;

#####create replicated metrics table type1
CREATE TABLE IF NOT EXISTS fidedw.ods_app_metrics_ed ON CLUSTER ck_replica3
(
	`metrics_time` DateTime,
    `metrics_name` String,
    `labels` Array(String),
    `value` Float64,
	`updated` DateTime DEFAULT now(),
    INDEX idx_app_metrics_label_set (labels, metrics_name) TYPE set(0) GRANULARITY 4
)
ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/all/fidedw/ods_app_metrics_ed', '{replica}', 'app_graphite_rollup')
PARTITION BY toYYYYMMDD(metrics_time)
ORDER BY (metrics_time, metrics_name, labels)
TTL metrics_time + INTERVAL 1 MONTH DELETE
;

#####create replicated metrics table type2
CREATE TABLE IF NOT EXISTS fidedw.ods_app_metrics_ed ON CLUSTER ck_replica3
(
	`metrics_time` DateTime CODEC(DoubleDelta, LZ4),
    `metrics_name` LowCardinality(String),
    `labels` Array(LowCardinality(String)),
    `value` Float64 CODEC(Gorilla, LZ4),
	`updated` DateTime DEFAULT now(),
    INDEX idx_app_metrics_label_set (labels, metrics_name) TYPE set(0) GRANULARITY 4
)
ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/all/fidedw/ods_app_metrics_ed', '{replica}', 'app_graphite_rollup')
PARTITION BY toYYYYMMDD(metrics_time)
ORDER BY (metrics_time, metrics_name, labels)
TTL metrics_time + INTERVAL 1 MONTH DELETE
;

