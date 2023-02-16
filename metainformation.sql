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


DROP event trigger IF EXISTS create_table_versioning_trigger ;
DROP FUNCTION IF EXISTS create_table_versioning_function();
CREATE OR REPLACE FUNCTION create_table_versioning_function()
RETURNS event_trigger
AS $$
DECLARE name varchar;
BEGIN
     SELECT object_identity INTO name FROM pg_event_trigger_ddl_commands() WHERE object_identity NOT LIKE '%_seq' AND object_identity NOT LIKE '%_pkey' AND object_identity NOT LIKE '%_hist' AND object_identity NOT LIKE '%versioned_tables%';
     IF name is null then
         RETURN;
     end if;
     RAISE NOTICE 'add_versioning to: % ', name;
     CALL add_versioning_hybrid(name);
END;
$$
LANGUAGE plpgsql;

CREATE EVENT TRIGGER create_table_versioning_trigger ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE')
    EXECUTE PROCEDURE create_table_versioning_function();

DROP event trigger IF EXISTS alter_table_detection ;
DROP FUNCTION IF EXISTS alter_table_detection_function();
CREATE OR REPLACE FUNCTION alter_table_detection_function()
RETURNS event_trigger
AS $$
DECLARE name varchar; obj record;
    query varchar;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        SELECT current_query() into query;
    END LOOP;
    RAISE NOTICE 'event % %', tg_event, tg_tag;
     SELECT object_identity INTO name FROM pg_event_trigger_ddl_commands() WHERE object_identity NOT LIKE '%_seq' AND object_identity NOT LIKE '%_pkey' AND object_identity NOT LIKE '%_hist' AND object_identity NOT LIKE 'hist.%';
     IF name is null then
         RETURN;
     end if;
    IF LOWER(query) like '%drop column%' then
        RAISE NOTICE 'drop column';
    end if;
    IF LOWER(query) like '%add%' then
        RAISE NOTICE 'add column in, %', name;
    end if;
    IF LOWER(query) like '%rename column%' then
        RAISE NOTICE 'rename, %', name;
    end if;
    IF Lower(query) like '%rename%' then
        RAISE NOTICE 'rename table, %', name;
    end if;
END;
$$
LANGUAGE plpgsql;

CREATE EVENT TRIGGER alter_table_detection ON ddl_command_end
    WHEN TAG IN ('ALTER TABLE')
    EXECUTE PROCEDURE alter_table_detection_function();

SELECT * from accounts;

ALTER TABLE accounts ADD firstname VARCHAR(255);