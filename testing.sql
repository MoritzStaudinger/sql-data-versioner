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




CALL add_versioning_integrated('accounts');
CALL add_versioning_separated('accounts');
CALL add_versioning_hybrid('accounts');
