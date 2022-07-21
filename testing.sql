DROP event trigger IF EXISTS drop_trigger;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS accounts_hist;
CREATE TABLE accounts (
	user_id serial,
	username VARCHAR ( 50 ),
	PRIMARY KEY(user_id)
);

INSERT INTO accounts(user_id, username)
VALUES(1,'test1'),(2,'test2');


INSERT INTO accounts(user_id, username)
VALUES(3,'test3'),(4,'test4');

--SELECT * from primary_keys WHERE table_name like 'accounts';

UPDATE accounts SET username='test1' WHERE user_id = 3;
DELETE FROM ACCOUNTS WHERE user_id = 2 AND username like 'test2';

SELECT * from accounts;
SELECT * from accounts_hist;




CALL add_versioning_integrated('public.accounts');
CALL add_versioning_separated('public.accounts');
CALL add_versioning_hybrid('public.accounts');

-- Querying


INSERT INTO download(id, timestamp, user_id)  VALUES(1,now(), 1);
INSERT INTO query(id, d_id, original_query) VALUES (1,1, 'SELECT * FROM accounts WHERE id = #i1 AND id = #i2 AND t like ''#s1'' AND test < #t1 ');
INSERT INTO parameters(id, q_id, int_array, string_array,timestamp_array) VALUES (1,1, ARRAY [1,0,4], ARRAY ['test'], ARRAY[now()]);

SELECT * FROM rebuild_query(1);

