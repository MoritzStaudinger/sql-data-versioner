
-- Procedure for Hybrid approach
DROP procedure if exists add_versioning_hybrid;
CREATE OR REPLACE procedure add_versioning_hybrid(tablename varchar)
language plpgsql
as $$
DECLARE
    primary_key  varchar;
    primary_key_conditions varchar;
    columnnames varchar;
    old_columnnames varchar;
    tablename_wo_schema varchar;
    schema_name varchar;
BEGIN
    IF tablename in (SELECT name FROM hist.versioned_tables) THEN
        RAISE EXCEPTION 'TABLE already versioned';
        RETURN;
    end if;
    SELECT SUBSTRING(tablename, POSITION('.' in tablename)+1) INTO tablename_wo_schema;
    SELECT SUBSTRING(tablename, 0, POSITION('.' in tablename)+1) INTO schema_name;

    EXECUTE format('ALTER TABLE %s ADD valid_from Timestamp;', tablename);
    EXECUTE format('ALTER TABLE %s ALTER COLUMN valid_from SET default now();', tablename);
    EXECUTE format('UPDATE %s SET valid_from = NOW() WHERE valid_from is NULL;', tablename);
    -- Create Hybrid
    EXECUTE format('CREATE TABLE hist.%1$s_hist(like %2$s);', tablename_wo_schema, tablename);
    EXECUTE format('ALTER TABLE hist.%s_hist ADD valid_to Timestamp;', tablename_wo_schema);

    --RAISE NOTICE '%', tablename_wo_schema;
    -- Change Primary Key for History table
    --RAISE NOTICE '%', tablename_wo_schema;
    SELECT STRING_agg(distinct column_name, ',')INTO primary_key FROM hist.primary_keys WHERE table_name like tablename_wo_schema;
    SELECT STRING_agg(cn.pk, ' AND ')INTO primary_key_conditions FROM (SELECT CONCAT(column_name, '=OLD.',column_name)as pk FROM hist.primary_keys WHERE table_name like tablename_wo_schema) as cn;
    EXECUTE format('ALTER TABLE hist.%1$s_hist ADD CONSTRAINT %1$s_hist_pkey primary key(%2$s,valid_from); ', tablename_wo_schema,primary_key);
    -- Create necessary triggers on insert, update and delete
    -- Extract Columnames
    SELECT STRING_agg(distinct cn.column_name, ',') INTO columnnames FROM (SELECT column_name FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;
    SELECT STRING_agg(distinct cn.column, ',') INTO old_columnnames FROM (SELECT CONCAT('OLD.',column_name) as column FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;

     -- Delete Rule
    EXECUTE format('CREATE RULE delete_from_%3$s AS ON DELETE TO %1$s ' ||
                   'DO ALSO (' ||
                   '    INSERT INTO hist.%3$s_hist SELECT * FROM %1$s WHERE %2$s;' ||
                   '    UPDATE hist.%3$s_hist SET valid_to = now() where %2$s;' ||
                   ');', tablename,primary_key_conditions, tablename_wo_schema);

    -- Update Rule
    EXECUTE format('CREATE OR REPLACE FUNCTION  update_trigger_function_%3$s() ' ||
                   'RETURNS TRIGGER ' ||
                   'LANGUAGE PLPGSQL ' ||
                   'AS $update_trigger_function$ ' ||
                   'BEGIN'
                   '    INSERT INTO hist.%3$s_hist(%1$s, valid_to) VALUES(%2$s, now());' ||
                   '    NEW.valid_from = now();' ||
                   '    RETURN NEW;' ||
                   'END; $update_trigger_function$ ', columnnames, old_columnnames, tablename_wo_schema);

    EXECUTE format('CREATE TRIGGER update_trigger_%2$s ' ||
                   'BEFORE UPDATE ' ||
                   'ON %1$s ' ||
                   'FOR EACH ROW ' ||
                   '    WHEN (pg_trigger_depth() < 1)' ||
                   '        EXECUTE PROCEDURE update_trigger_function_%2$s();', tablename, tablename_wo_schema);

    EXECUTE format('INSERT INTO hist.versioned_tables(name) VALUES(''%s'');', tablename);
END
$$;

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
            CALL add_versioning_hybrid(t);
        END LOOP;

    END
    $$;
end;