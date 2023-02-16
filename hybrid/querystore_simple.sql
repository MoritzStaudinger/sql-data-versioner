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
    re_execute_query text,
    query_hash varchar(1024),
    result_nr INTEGER,
    result_hash varchar,
    CONSTRAINT fk_download_simple
        FOREIGN KEY(d_id)
        REFERENCES download_simple(id)
);

DROP function if exists save_download_simple;
CREATE OR REPLACE function save_download_simple(ts timestamp, u_id integer)
RETURNS integer
LANGUAGE plpgsql
AS
    $$
    DECLARE
        download_id integer;
    BEGIN
         INSERT INTO download_simple(timestamp, user_id) VALUES(ts, u_id);
         SELECT d.id INTO download_id FROM download_simple d WHERE d.user_id = u_id ORDER BY d.id desc LIMIT 1;
         RETURN download_id;
    END
    $$;
end;



DROP procedure if exists save_query_simple;
CREATE OR REPLACE procedure save_query_simple(query text, re_execute text, result_nr integer, result_hash text, download_id integer)
LANGUAGE plpgsql
AS
    $$
    BEGIN

        INSERT INTO query_simple(d_id, original_query, re_execute_query, query_hash, result_nr, result_hash)

        VALUES(download_id, query, re_execute, sha512(query::bytea), result_nr, result_hash);
    END
    $$;
end;
