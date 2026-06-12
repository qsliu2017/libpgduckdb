-- Test ducklake.set_commit_message() procedure

CREATE TABLE cm_test (a int) USING ducklake;

-- Set commit message before making changes
CALL ducklake.set_commit_message('test_user', 'initial data load');
INSERT INTO cm_test VALUES (1), (2), (3);

SELECT * FROM cm_test ORDER BY a;

-- Cannot assert the message itself: snapshots() returns SETOF duckdb.row,
-- so columns are not addressable by name in PG.

CALL ducklake.set_commit_message('another_user', 'add more data');
INSERT INTO cm_test VALUES (4), (5);

SELECT * FROM cm_test ORDER BY a;

-- Cleanup
DROP TABLE cm_test;
