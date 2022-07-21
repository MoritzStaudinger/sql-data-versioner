DROP VIEW IF EXISTS primary_keys;
CREATE VIEW primary_keys AS
SELECT tc.table_name,
    c.column_name
   FROM information_schema.table_constraints tc
     JOIN information_schema.constraint_column_usage ccu USING (constraint_schema, constraint_name)
     JOIN information_schema.columns c ON c.table_schema::name = tc.constraint_schema::name AND tc.table_name::name = c.table_name::name AND ccu.column_name::name = c.column_name::name
  WHERE tc.constraint_type::text = 'PRIMARY KEY'::text;

DROP TABLE IF EXISTS versioned_tables;
CREATE TABLE versioned_tables(
    name VARCHAR(100),
    PRIMARY KEY (name)
);

DROP event trigger IF EXISTS drop_trigger;
DROP FUNCTION IF EXISTS drop_trigger_function();
CREATE FUNCTION drop_trigger_function() RETURNS event_trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    obj record;
    tablename varchar;
BEGIN
    --SELECT STRING_agg(ob) INTO tablename FROM (SELECT * FROM pg_event_trigger_ddl_commands() LIMIT 1) as ob;
    --RAISE NOTICE '%', (SELECT objid::regclass FROM pg_event_trigger_ddl_commands());
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        RAISE NOTICE ' % caught %', obj.command_tag, obj.object_identity;
    END LOOP;
    IF (tablename not like '%_hist') THEN
        RAISE NOTICE 'INSERT INTO %_hist SELECT * from % ', tablename, tablename;
        EXECUTE format('INSERT INTO %s_hist SELECT * FROM %s;', tablename, tablename);
        RAISE NOTICE '%', tablename;
    ELSE
        RAISE NOTICE 'Nothing';
    END IF;
END;
$$;
END;

CREATE EVENT TRIGGER drop_trigger ON ddl_command_start
    WHEN TAG IN ('DROP TABLE')
    EXECUTE FUNCTION drop_trigger_function();

DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS accounts_hist;

CREATE TABLE accounts (
	user_id serial,
	username VARCHAR ( 50 ),
	PRIMARY KEY(user_id)
);

DROP event trigger create_versioning ;
DROP FUNCTION create_versioning_function();
CREATE OR REPLACE FUNCTION create_versioning_function()
RETURNS event_trigger
AS $$
DECLARE name text;
BEGIN
     SELECT object_identity INTO name FROM pg_event_trigger_ddl_commands() WHERE object_identity NOT LIKE '%_seq' AND object_identity NOT LIKE '%_pkey' AND object_identity NOT LIKE '%_hist' LIMIT 1;
      RAISE NOTICE 'add_versioning to: % ', name;
      CALL add_versioning_hybrid(name);
END;
$$
LANGUAGE plpgsql;

CREATE EVENT TRIGGER create_versioning ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE')
    EXECUTE PROCEDURE create_versioning_function();
