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