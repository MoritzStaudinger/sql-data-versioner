CREATE SCHEMA hist;

DROP VIEW IF EXISTS hist.primary_keys;
CREATE VIEW hist.primary_keys AS
SELECT tc.table_name,
    c.column_name
   FROM information_schema.table_constraints tc
     JOIN information_schema.constraint_column_usage ccu USING (constraint_schema, constraint_name)
     JOIN information_schema.columns c ON c.table_schema::name = tc.constraint_schema::name AND tc.table_name::name = c.table_name::name AND ccu.column_name::name = c.column_name::name
  WHERE tc.constraint_type::text = 'PRIMARY KEY'::text;

DROP TABLE IF EXISTS hist.versioned_tables;
CREATE TABLE hist.versioned_tables(
    name VARCHAR(100),
    PRIMARY KEY (name)
);




