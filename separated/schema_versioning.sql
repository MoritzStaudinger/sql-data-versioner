
DROP event trigger IF EXISTS drop_trigger;
DROP FUNCTION IF EXISTS drop_trigger_function();
CREATE FUNCTION drop_trigger_function() RETURNS event_trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    obj record;
    tablename varchar;
    query varchar;
    name varchar;
    date varchar;
BEGIN
    SELECT current_query() into query;
    SELECT TO_CHAR(NOW() :: DATE, 'yyyymmdd') into date;
    RAISE NOTICE 'query: %', query;
    IF LOWER(query) like '%drop table%' then
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('DROP TABLE '), 2),';',1) into name;
            SELECT SPLIT_PART(name, '.', 2) INTO tablename;
            IF (tablename not like '%_hist%') THEN
                RAISE NOTICE 'ALTER TABLE hist.accounts_hist RENAME TO %_hist_% ;',tablename,date;
                execute format('ALTER TABLE hist.%1$s_hist RENAME TO %1$s_hist_%2$s ;',tablename,date);
                RAISE Notice 'test';
                execute format('UPDATE query_simple SET re_execute_query = REPLACE(re_execute_query, ''hist.%1$s_hist'', ''hist.%1$s_hist_%2$s'') WHERE re_execute_query like ''%%hist.%1$s%%'' ', tablename,date);
            end if;
    end if;
END;
$$;
END;

CREATE EVENT TRIGGER drop_trigger ON ddl_command_end
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
     CALL add_versioning_separated(name);
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
        call drop_column_event(query);
        RAISE NOTICE 'drop column';
    end if;
    IF LOWER(query) like '%add%' then
        call add_column_event(query);
        RAISE NOTICE 'add column in, %', name;
    end if;
    IF LOWER(query) like '%rename column%' then
        call rename_column_event();
        RAISE NOTICE 'rename, %', name;
    end if;
    IF Lower(query) like '%rename to%' then
        call rename_table_event(query);
        RAISE NOTICE 'rename table, %', name;
    end if;
END;
$$
LANGUAGE plpgsql;

CREATE EVENT TRIGGER alter_table_detection ON ddl_command_end
    WHEN TAG IN ('ALTER TABLE')
    EXECUTE PROCEDURE alter_table_detection_function();



DROP procedure if exists rename_column_event;
CREATE OR REPLACE procedure rename_column_event(query text)
LANGUAGE plpgsql
AS
    $$
    DECLARE name varchar;
        orig_name varchar;
        columnnames varchar;
        orig_column varchar;
        schemaname varchar;
        new_column varchar;
    BEGIN
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('ALTER TABLE '), 2),lower(' RENAME '),1) into name;
            SELECT SPLIT_PART(name, '.', 2) INTO orig_name;
            SELECT SPLIT_PART(name, '.', 1) INTO schemaname;
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('RENAME '), 2),';',1) into columnnames;
            SELECT SPLIT_PART(lower(columnnames), lower(' TO'), 1) into orig_column;
            SELECT SPLIT_PART(lower(columnnames), lower('TO '), 2) into new_column;
            IF (orig_name not like '%_hist%') THEN
                execute format('ALTER TABLE hist.%1$s_hist RENAME %2$s_hist', orig_name, columnnames);
                call adapt_triggers(orig_name, schemaname,orig_name);
                -- Rudimentary rewrite. needs to be adapted for table.column and table-abbreviation.column and normal columnname. This can lead to problems with queries, when two tables have the same name for a column
                execute format('UPDATE query_simple SET re_execute_query = REPLACE(re_execute_query, ''%1$s'', ''%2$s'') WHERE re_execute_query like ''%%%1$s%%'' ', orig_column,new_column);

            end if;
    END
    $$;
end;

DROP procedure if exists add_column_event;
CREATE OR REPLACE procedure add_column_event(query text)
LANGUAGE plpgsql
AS
    $$
    DECLARE name varchar;
        orig_name varchar;
        columnname varchar;
        schemaname varchar;
    BEGIN
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('ALTER TABLE '), 2),lower(' ADD COLUMN '),1) into name;
            SELECT SPLIT_PART(name, '.', 2) INTO orig_name;
            SELECT SPLIT_PART(name, '.', 1) INTO schemaname;
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('ADD COLUMN '), 2),';',1) into columnname;


            IF (orig_name not like '%_hist%') THEN
                execute format('ALTER TABLE hist.%1$s_hist ADD COLUMN %2$s', orig_name, columnname);
                call adapt_triggers(orig_name, schemaname,orig_name);
                -- no rewriting necessary
            end if;
    END
    $$;
end;

DROP procedure if exists drop_column_event;
CREATE OR REPLACE procedure drop_column_event(query text)
LANGUAGE plpgsql
AS
    $$
    DECLARE name varchar;
        orig_name varchar;
        columnname varchar;
    BEGIN
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('ALTER TABLE '), 2),lower(' DROP COLUMN '),1) into name;
            SELECT SPLIT_PART(name, '.', 2) INTO orig_name;
            IF (orig_name not like '%_hist%') THEN
                call adapt_triggers(orig_name, 'public',orig_name);
                -- no rewriting necessary
            end if;

    END
    $$;
end;



DROP procedure if exists rename_table_event;
CREATE OR REPLACE procedure rename_table_event(query text)
LANGUAGE plpgsql
AS
    $$
    DECLARE name varchar;
        orig_name varchar;
        new_name varchar;
    BEGIN
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('ALTER TABLE '), 2),lower(' RENAME TO'),1) into name;
            SELECT SPLIT_PART(name, '.', 2) INTO orig_name;
            SELECT SPLIT_PART(SPLIT_PART(lower(query), lower('RENAME TO '), 2),';',1) into new_name;
            IF (orig_name not like '%_hist%') THEN
                execute format('ALTER TABLE hist.%1$s_hist RENAME TO %2$s_hist', orig_name, new_name);
                call adapt_triggers(orig_name, 'public',new_name);

                execute format('UPDATE query_simple SET re_execute_query = REPLACE(re_execute_query, ''hist.%1$s_hist'', ''hist.%2$s_hist'') WHERE re_execute_query like ''%%hist.%1$s%%'' ', orig_name,new_name);

            end if;
    END
    $$;
end;

CREATE OR REPLACE procedure adapt_triggers(tablename text, schemaname text, new_tablename text)
LANGUAGE plpgsql
AS
    $$
    DECLARE
    primary_key  varchar;
    primary_key_conditions varchar;
    primary_key_conditions_new varchar;
    columnnames varchar;
    old_columnnames varchar;
    new_columnnames varchar;
    column_reset varchar;
    BEGIN
    EXECUTE format('DROP rule delete_from_%3$s ON %2$s.%1$s', new_tablename, schemaname,tablename);
    EXECUTE format('DROP trigger update_trigger_%3$s ON %2$s.%1$s',new_tablename, schemaname,tablename);
    EXECUTE format('DROP trigger insert_trigger_%3$s ON %2$s.%1$s',new_tablename, schemaname, tablename);

    SELECT STRING_agg(DISTINCT column_name, ',')INTO primary_key FROM hist.primary_keys WHERE table_name like new_tablename;
    SELECT STRING_agg(DISTINCT cn.pk, ' AND ')INTO primary_key_conditions FROM (SELECT CONCAT(column_name, '=OLD.',column_name)as pk FROM hist.primary_keys WHERE table_name like new_tablename) as cn;
    SELECT STRING_agg(DISTINCT cn.pk, ' AND ')
    INTO primary_key_conditions_new
    FROM (SELECT CONCAT(column_name, '=NEW.', column_name) as pk
          FROM hist.primary_keys
          WHERE table_name like tablename) as cn;
    -- Create necessary triggers on insert, update and delete
    -- Extract Columnames
    SELECT STRING_agg(distinct cn.column_name, ',') INTO columnnames FROM (SELECT column_name FROM information_schema.columns WHERE table_name like new_tablename AND column_name <> 'valid_to' ORDER BY column_Name) as cn;
    SELECT STRING_agg(distinct cn.column, ',') INTO old_columnnames FROM (SELECT CONCAT('OLD.',column_name) as column FROM information_schema.columns WHERE table_name like new_tablename AND column_name <> 'valid_to' ORDER BY column_Name) as cn;
    SELECT STRING_agg(DISTINCT cn.column, ',') INTO new_columnnames FROM (SELECT CONCAT('NEW.',column_name) as column FROM information_schema.columns WHERE table_name like new_tablename AND column_name <> 'valid_to' AND column_name <> 'valid_from' ORDER BY column_Name) as cn;
    SELECT STRING_agg(concat(cn.ncolumn, '=', cn.ocolumn), ';') INTO column_reset FROM (SELECT CONCAT('OLD.',column_name) as ocolumn, CONCAT('NEW.', column_name) as ncolumn FROM information_schema.columns WHERE table_name like new_tablename AND column_name <> 'valid_to' ORDER BY column_Name) as cn;

-- Delete Rule
    EXECUTE format('CREATE RULE delete_from_%3$s AS ON DELETE TO %1$s.%3$s ' ||
                   'DO ALSO (' ||
                   '    UPDATE hist.%3$s_hist SET valid_to = now() where %2$s;' ||
                   ');', schemaname,primary_key_conditions, new_tablename);

        -- Update Rule
    EXECUTE format('CREATE OR REPLACE FUNCTION  update_trigger_function_%3$s() ' ||
                   'RETURNS TRIGGER ' ||
                   'LANGUAGE PLPGSQL ' ||
                   'AS $update_trigger_function$ ' ||
                   'BEGIN' ||
                   '    UPDATE hist.%3$s_hist SET valid_to = now() WHERE %6$s ;' ||
                   '    INSERT INTO hist.%3$s_hist(%1$s) VALUES(%2$s);' ||
                   '    %4$s;' ||
                   '    RETURN NEW;' ||
                   'END; $update_trigger_function$ ', columnnames, new_columnnames, new_tablename,column_reset,schemaname, primary_key_conditions);

    EXECUTE format('CREATE TRIGGER update_trigger_%2$s ' ||
                   'AFTER UPDATE ' ||
                   'ON %1$s.%2$s ' ||
                   'FOR EACH ROW ' ||
                   '    WHEN (pg_trigger_depth() < 1)' ||
                   '        EXECUTE PROCEDURE update_trigger_function_%2$s();', schemaname, new_tablename);


    -- Insert Rule
    EXECUTE format('CREATE OR REPLACE FUNCTION  insert_trigger_function_%3$s() ' ||
                   'RETURNS TRIGGER ' ||
                   'LANGUAGE PLPGSQL ' ||
                   'AS $insert_trigger_function$ ' ||
                   'BEGIN' ||
                   '    INSERT INTO hist.%3$s_hist(%1$s) VALUES(%2$s);' ||
                   '    RETURN NEW;' ||
                   'END; $insert_trigger_function$ ', columnnames, new_columnnames, new_tablename);

    EXECUTE format('CREATE TRIGGER insert_trigger_%2$s ' ||
                   'AFTER INSERT ' ||
                   'ON %1$s.%2$s ' ||
                   'FOR EACH ROW ' ||
                   '    WHEN (pg_trigger_depth() < 1)' ||
                   '        EXECUTE PROCEDURE insert_trigger_function_%2$s();', schemaname, new_tablename);
    end;
$$
end;
