-- simple deletion

CREATE TABLE test2 (ID BIGINT NOT NULL, TMSTAMP TIMESTAMP NOT NULL, PRIMARY KEY (ID));
INSERT INTO test2 (ID) VALUES (1);
INSERT INTO test2 (ID) VALUES (2);
INSERT INTO test2 (ID) VALUES (3);
INSERT INTO test2 (ID) VALUES (4);
INSERT INTO test2 (ID) VALUES (5);
DELETE FROM test2 WHERE ID = 1;
DELETE FROM test2 WHERE ID = 3;
DELETE FROM test2 WHERE ID = 5;