DROP TABLE IF EXISTS upsert_test_multi_unique; 
CREATE TABLE upsert_test_multi_unique(time timestamp NOT NULL, temp float, color text);
CREATE INDEX multi_time_idx ON upsert_test_multi_unique (time);
CREATE UNIQUE INDEX multi_time_temp_idx ON upsert_test_multi_unique (time, temp);
CREATE UNIQUE INDEX multi_time_color_idx ON upsert_test_multi_unique (time, color);
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 25.9, 'yellow');
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-21T09:00:01', 25.9, 'yellow');
INSERT INTO upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'brown');

DROP TABLE IF EXISTS ht_upsert_test_multi_unique; 
CREATE TABLE ht_upsert_test_multi_unique(time timestamp NOT NULL, temp float, color text);
SELECT create_hypertable('ht_upsert_test_multi_unique', 'time');
CREATE UNIQUE INDEX ht_multi_time_temp_idx ON ht_upsert_test_multi_unique (time, temp);
CREATE UNIQUE INDEX ht_multi_time_color_idx ON ht_upsert_test_multi_unique (time, color);
INSERT INTO ht_upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 25.9, 'yellow');
INSERT INTO ht_upsert_test_multi_unique VALUES ('2017-01-21T09:00:01', 25.9, 'yellow');
INSERT INTO ht_upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'brown');

 --UPDATE ht_upsert_test_multi_unique  SET temp = 23.5 where time ='2017-01-20T09:00:01' and color = 'yellow';
 --INSERT INTO ht_upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'yellow') ON CONFLICT (time, color) DO UPDATE SET temp = 23.5;


DROP TABLE IF EXISTS dp_upsert_test_multi_unique CASCADE; 

CREATE TABLE dp_upsert_test_multi_unique(time timestamp NOT NULL, temp float, color text) PARTITION BY range(time);
create table dp_test_1 partition of dp_upsert_test_multi_unique for values from ('2017-01-20') TO ('2017-01-20T23:59:59');
create table dp_test_2 partition of dp_upsert_test_multi_unique for values from ('2017-01-21') TO ('2017-01-21T23:59:59');
CREATE INDEX dp_multi_time_idx ON dp_upsert_test_multi_unique (time);
CREATE UNIQUE INDEX dp_multi_time_temp_idx ON dp_upsert_test_multi_unique (time, temp);
CREATE UNIQUE INDEX dp_multi_time_color_idx ON dp_upsert_test_multi_unique (time, color);
INSERT INTO dp_upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 25.9, 'yellow');
INSERT INTO dp_upsert_test_multi_unique VALUES ('2017-01-21T09:00:01', 25.9, 'yellow');
INSERT INTO dp_upsert_test_multi_unique VALUES ('2017-01-20T09:00:01', 23.5, 'brown');