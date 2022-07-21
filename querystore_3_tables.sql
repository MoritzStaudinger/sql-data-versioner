DROP TABLE IF EXISTS parameters;
DROP TABLE IF EXISTS query;
DROP TABLE IF EXISTS download;
CREATE TABLE IF NOT EXISTS download (
    id serial primary key,
    timestamp TIMESTAMP,
    user_id serial
);




CREATE TABLE IF NOT EXISTS query (
    id serial PRIMARY KEY,
    d_id serial,
    doi text,
    original_query text,
    normalized_query text,
    query_hash varchar(1024),
    result_nr INTEGER,
    result_hash varchar,
    CONSTRAINT fk_download
        FOREIGN KEY(d_id)
        REFERENCES download(id)
);

--select pg_get_viewdef('primary_keys', true);


CREATE TABLE IF NOT EXISTS parameters (
    id serial primary key,
    q_id serial,
    int_array integer[],
    string_array varchar[],
    timestamp_array varchar[],
        CONSTRAINT fk_query
        FOREIGN KEY(q_id)
        REFERENCES query(id)
);

DROP function if exists rebuild_query;
CREATE OR REPLACE function rebuild_query(p_id integer)
RETURNS text
LANGUAGE plpgsql
AS
    $$
    DECLARE
        query_string varchar;
        params parameters;
        i integer;
        s varchar;
        t timestamp;
        c integer = 1;
    BEGIN
        SELECT q.original_query INTO query_string FROM query q INNER JOIN parameters p ON q.id = p.q_id WHERE p.id = p_id;
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

