-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

CREATE TABLE reloptions_test(time integer, temp float8, color integer)
WITH (fillfactor=75, oids=true, autovacuum_vacuum_threshold=100);

SELECT create_hypertable('reloptions_test', 'time', chunk_time_interval => 3);

INSERT INTO reloptions_test VALUES (4, 24.3, 1), (9, 13.3, 2);

-- Show that reloptions are inherited by chunks
SELECT relname, reloptions, relhasoids FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

-- Alter reloptions
ALTER TABLE reloptions_test SET (fillfactor=80, parallel_workers=8);

SELECT relname, reloptions, relhasoids FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

ALTER TABLE reloptions_test RESET (fillfactor);

SELECT relname, reloptions, relhasoids FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

ALTER TABLE reloptions_test SET WITHOUT OIDS;

SELECT relname, reloptions, relhasoids FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

ALTER TABLE reloptions_test SET WITH OIDS;

SELECT relname, reloptions, relhasoids FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';
