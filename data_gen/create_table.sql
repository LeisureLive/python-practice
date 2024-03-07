DROP TABLE IF EXISTS default.user_data;
CREATE EXTERNAL TABLE default.user_data (
id bigint,
anonymous_id string,
login_id string,
identities string,
properties string
)
STORED AS PARQUET
LOCATION '@data_path/user_data';

DROP TABLE IF EXISTS default.event_login_data;
CREATE EXTERNAL TABLE default.event_login_data (
id bigint,
event string,
time bigint,
anonymous_id string,
login_id string,
identities string,
properties string
)
STORED AS PARQUET
LOCATION '@data_path/event_login_data';

DROP TABLE IF EXISTS default.event_mixed_data;
CREATE EXTERNAL TABLE default.event_mixed_data (
id bigint,
event string,
time bigint,
anonymous_id string,
login_id string,
identities string,
properties string
)
STORED AS PARQUET
LOCATION '@data_path/event_mixed_data';