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
DROP FUNCTION IF EXISTS drop_trigger();
CREATE FUNCTION drop_trigger() RETURNS event_trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    obj record;
    tablename varchar;
BEGIN
    -- SELECT STRING_agg(ob) INTO tablename FROM (SELECT * FROM pg_event_trigger_ddl_commands() LIMIT 1) as ob; -->
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        RAISE NOTICE 'caught % event on %', obj.command_tag, obj.object_identity;
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

CREATE EVENT TRIGGER drop_trigger ON ddl_command_start
    WHEN TAG IN ('DROP TABLE')
    EXECUTE FUNCTION drop_trigger();