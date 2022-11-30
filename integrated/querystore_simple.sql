DROP TABLE IF EXISTS query_simple;
DROP TABLE IF EXISTS download_simple;
CREATE TABLE IF NOT EXISTS download_simple(
    id serial primary key,
    timestamp TIMESTAMP,
    user_id serial
);

CREATE TABLE IF NOT EXISTS query_simple (
    id serial PRIMARY KEY,
    d_id serial,
    doi text,
    original_query text,
    normalized_query text,
    query_hash varchar(1024),
    result_nr INTEGER,
    result_hash varchar,
    CONSTRAINT fk_download_simple
        FOREIGN KEY(d_id)
        REFERENCES download_simple(id)
);

DROP procedure if exists save_download_simple;
CREATE OR REPLACE procedure save_download_simple(ts timestamp, user_id integer)
LANGUAGE plpgsql
AS
    $$
    BEGIN
         INSERT INTO download_simple(timestamp, user_id) VALUES(ts, user_id);
    END
    $$;
end;

CALL save_download_simple(TIMESTAMP '2004-10-19 10:23:54', 1);

DROP procedure if exists save_query_simple;
CREATE OR REPLACE procedure save_query_simple(query text, result_nr integer, result_hash text, u_id integer)
LANGUAGE plpgsql
AS
    $$
    DECLARE
        download_id integer;
        query_hash text;
    BEGIN
        SELECT d.id INTO download_id FROM download_simple d WHERE d.user_id = u_id ORDER BY d.id desc LIMIT 1;

        INSERT INTO query_simple(d_id, original_query, normalized_query, query_hash, result_nr, result_hash)
        VALUES(download_id, query, query, md5(query), result_nr, result_hash);
    END
    $$;
end;

CALL save_query_simple('SELECT id FROM downloads', 1, md5('SELECT id FROM downloads'), 1);

SELECT * from query_simple;