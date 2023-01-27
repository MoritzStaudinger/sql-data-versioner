DROP procedure if exists add_versioning_separated;
CREATE OR REPLACE procedure add_versioning_separated(tablename varchar)
language plpgsql
as $$
DECLARE
    primary_key  varchar;
    primary_key_conditions varchar;
    primary_key_conditions_new varchar;
    columnnames varchar;
    old_columnnames varchar;
    tablename_wo_schema varchar;
    new_columnnames varchar;
    schema_name varchar;
    column_reset varchar;
BEGIN
    SELECT SUBSTRING(tablename, POSITION('.' in tablename)+1) INTO tablename_wo_schema;
    SELECT SUBSTRING(tablename, 0, POSITION('.' in tablename)+1) INTO schema_name;
        -- Create Hybrid/Separated Table
    EXECUTE format('CREATE TABLE hist.%s_hist(like %s);', tablename_wo_schema, tablename);
    -- Add valid_from column
    EXECUTE format('ALTER TABLE hist.%s_hist ADD valid_from Timestamp;', tablename_wo_schema);
    EXECUTE format('ALTER TABLE hist.%s_hist ALTER COLUMN valid_from SET default now();', tablename_wo_schema);
    -- Does NOW() behave correctly, otherwise replace with trigger
    EXECUTE format('INSERT INTO hist.%s_hist SELECT * FROM %s;', tablename_wo_schema, tablename);
    EXECUTE format('ALTER TABLE hist.%s_hist ADD valid_to Timestamp;', tablename_wo_schema);
    -- Change Primary Key for History table
    SELECT STRING_agg(DISTINCT column_name, ',')INTO primary_key FROM hist.primary_keys WHERE table_name like tablename_wo_schema;
    SELECT STRING_agg(DISTINCT cn.pk, ' AND ')INTO primary_key_conditions FROM (SELECT CONCAT(column_name, '=OLD.',column_name)as pk FROM hist.primary_keys WHERE table_name like tablename_wo_schema) as cn;
    SELECT STRING_agg(DISTINCT cn.pk, ' AND ')
    INTO primary_key_conditions_new
    FROM (SELECT CONCAT(column_name, '=NEW.', column_name) as pk
          FROM hist.primary_keys
          WHERE table_name like tablename_wo_schema) as cn;

    EXECUTE format('ALTER TABLE hist.%1$s_hist ADD CONSTRAINT %1$s_hist_pkey primary key(%2$s,valid_from); ',
                   tablename_wo_schema, primary_key);

    -- Create necessary triggers on insert, update and delete
    -- Extract Columnames
    SELECT STRING_agg(distinct cn.column_name, ',') INTO columnnames FROM (SELECT column_name FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;
    SELECT STRING_agg(distinct cn.column, ',') INTO old_columnnames FROM (SELECT CONCAT('OLD.',column_name) as column FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;
    SELECT STRING_agg(DISTINCT cn.column, ',') INTO new_columnnames FROM (SELECT CONCAT('NEW.',column_name) as column FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' AND column_name <> 'valid_from' ORDER BY column_Name) as cn;
    SELECT STRING_agg(concat(cn.ncolumn, '=', cn.ocolumn), ';') INTO column_reset FROM (SELECT CONCAT('OLD.',column_name) as ocolumn, CONCAT('NEW.', column_name) as ncolumn FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;

-- Delete Rule
    EXECUTE format('CREATE RULE delete_from_%3$s AS ON DELETE TO %1$s ' ||
                   'DO ALSO (' ||
                   '    UPDATE hist.%3$s_hist SET valid_to = now() where %2$s;' ||
                   ');', tablename,primary_key_conditions, tablename_wo_schema);

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
                   'END; $update_trigger_function$ ', columnnames, new_columnnames, tablename_wo_schema,column_reset,schema_name, primary_key_conditions);

    EXECUTE format('CREATE TRIGGER update_trigger_%2$s ' ||
                   'AFTER UPDATE ' ||
                   'ON %1$s ' ||
                   'FOR EACH ROW ' ||
                   '    WHEN (pg_trigger_depth() < 1)' ||
                   '        EXECUTE PROCEDURE update_trigger_function_%2$s();', tablename, tablename_wo_schema);


    -- Insert Rule
    EXECUTE format('CREATE OR REPLACE FUNCTION  insert_trigger_function_%3$s() ' ||
                   'RETURNS TRIGGER ' ||
                   'LANGUAGE PLPGSQL ' ||
                   'AS $insert_trigger_function$ ' ||
                   'BEGIN' ||
                   '    INSERT INTO hist.%3$s_hist(%1$s) VALUES(%2$s);' ||
                   '    RETURN NEW;' ||
                   'END; $insert_trigger_function$ ', columnnames, new_columnnames, tablename_wo_schema);

    EXECUTE format('CREATE TRIGGER insert_trigger_%2$s ' ||
                   'AFTER INSERT ' ||
                   'ON %1$s ' ||
                   'FOR EACH ROW ' ||
                   '    WHEN (pg_trigger_depth() < 1)' ||
                   '        EXECUTE PROCEDURE insert_trigger_function_%2$s();', tablename, tablename_wo_schema);


    EXECUTE format('INSERT INTO hist.versioned_tables(name) VALUES(''%s'');', tablename);
END
$$;

--CALL add_versioning_separated('data.station_image');
DROP procedure if exists add_all_versioning();
CREATE OR REPLACE procedure add_all_versioning()
LANGUAGE plpgsql
AS
    $$
    DECLARE
        table_names varchar[];
        altertable varchar[];
        t varchar;
        r record;

    BEGIN
        ALTER TABLE data.station_image ADD PRIMARY KEY (station_id, image_id);
        SELECT array_agg(concat(concat(concat(concat('ALTER TABLE ', concat(concat(constraint_schema, '.'), table_name),' '), 'DROP CONSTRAINT '), quote_ident(constraint_name)),';')) INTO altertable FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE constraint_schema like 'data' and constraint_name like '%fkey';

        FOREACH t IN ARRAY altertable LOOP
           raise notice '%', t;
            EXECUTE FORMAT(t);
        end loop;

        SELECT array_agg(concat(concat(concat(concat('ALTER TABLE ', concat(concat(constraint_schema, '.'), table_name),' '), 'DROP CONSTRAINT '), constraint_name),';')) INTO altertable FROM INFORMATION_SCHEMA.table_constraints WHERE constraint_schema like 'data' and constraint_type like 'CHECK' and constraint_name not like '%_not_null';
        FOREACH t IN ARRAY altertable LOOP
            raise notice '%', t;
            EXECUTE FORMAT(t);
        end loop;
         RAISE NOTICE 'start';
        SELECT array_agg(CONCAT('data.',table_name)) INTO table_names FROM information_schema.tables WHERE table_schema = 'data';
        FOREACH t IN ARRAY table_names LOOP
            RAISE NOTICE '%', t;
            CALL add_versioning_separated(t);
        END LOOP;

    END
    $$;
end;


