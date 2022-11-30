DROP TABLE IF EXISTS parameters;
DROP TABLE IF EXISTS query_advanced;
DROP TABLE IF EXISTS download_advanced;
CREATE TABLE IF NOT EXISTS download_advanced (
    id serial primary key,
    timestamp TIMESTAMP,
    user_id serial
);


CREATE TABLE IF NOT EXISTS query_advanced (
    id serial PRIMARY KEY,
    d_id serial,
    doi text,
    original_query text,
    normalized_query text,
    query_hash varchar(1024),
    CONSTRAINT fk_download
        FOREIGN KEY(d_id)
        REFERENCES download_advanced(id)
);

--select pg_get_viewdef('primary_keys', true);


CREATE TABLE IF NOT EXISTS parameters (
    id serial primary key,
    q_id serial,
    result_nr INTEGER,
    result_hash varchar,
    int_array integer[],
    string_array varchar[],
    timestamp_array varchar[],
        CONSTRAINT fk_query
        FOREIGN KEY(q_id)
        REFERENCES query_advanced(id)
);

DROP function if exists rebuild_query;
CREATE OR REPLACE function rebuild_query(p_id integer)
RETURNS text
LANGUAGE plpgsql
AS
    $$
    DECLARE
        query_string text;
        params parameters;
        i integer;
        s varchar;
        t timestamp;
        c integer = 1;
    BEGIN
        SELECT q.original_query INTO query_string FROM query_advanced q INNER JOIN parameters p ON q.id = p.q_id WHERE p.id = p_id;
        SELECT * INTO params FROM parameters p WHERE p.id = p_id;
        FOREACH i IN ARRAY params.int_array LOOP
            RAISE NOTICE '%: %', c, i;
            SELECT REPLACE(query_string, CONCAT('#i',c::text), i::text) INTO query_string;
            c = c+1;
        END LOOP;
        c = 1;
        FOREACH s IN ARRAY params.string_array LOOP
            RAISE NOTICE '%: %', c, s;
            SELECT REPLACE(query_string, CONCAT('#s',c::text), s) INTO query_string;
            c = c+1;
        END LOOP;
        c = 1;
        FOREACH t IN ARRAY params.timestamp_array LOOP
            RAISE NOTICE '%: %', c, t;
            SELECT REPLACE(query_string, CONCAT('#t',c::text), t::text) INTO query_string;
            c = c+1;
        END LOOP;
        return query_string;
    END
    $$;
end;

DROP procedure if exists save_download_advanced;
CREATE OR REPLACE procedure save_download_advanced(ts timestamp, user_id integer)
LANGUAGE plpgsql
AS
    $$
    BEGIN
         INSERT INTO download_advanced(timestamp, user_id) VALUES(ts, user_id);
    END
    $$;
end;

CALL save_download_advanced(TIMESTAMP '2004-10-19 10:23:54', 1);

DROP procedure if exists save_query_advanced(text, integer);
CREATE OR REPLACE procedure save_query_advanced(query text, u_id integer)
LANGUAGE plpgsql
AS
    $$
    DECLARE
        download_id integer;
        query_id integer;
    BEGIN
        SELECT d.id INTO download_id FROM download_advanced d WHERE d.user_id = u_id ORDER BY d.id desc LIMIT 1;

        INSERT INTO query_advanced(d_id, original_query, normalized_query, query_hash)
        VALUES(download_id, query, query, md5(query));
    END
    $$;
end;
CALL save_query_advanced('SELECT * FROM accounts WHERE id = #i1 AND id = #i2 AND t like #s1 AND test < #t1',1 );

DROP procedure if exists save_parameters;
CREATE OR REPLACE procedure save_parameters(u_id integer, result_nr integer, result_hash text, int_array integer[], string_array varchar[], timestamp_array varchar[])
LANGUAGE plpgsql
AS
    $$
    DECLARE
        query_id integer;
    BEGIN

        SELECT q.id INTO query_id FROM download_advanced d INNER JOIN query_advanced q ON q.d_id = d.id WHERE d.user_id = u_id ORDER BY q.id desc LIMIT 1;

        INSERT INTO parameters(q_id, result_nr, result_hash, int_array, string_array,timestamp_array) VALUES(query_id, result_nr, result_hash,int_array, string_array, timestamp_array);
    END
    $$;
end;

CALL save_parameters(1, 1,md5('Result'),ARRAY [1,0,4], ARRAY ['test'], ARRAY['2004-10-19 10:23:54'] );


select * from query_advanced;
select rebuild_query(1);


