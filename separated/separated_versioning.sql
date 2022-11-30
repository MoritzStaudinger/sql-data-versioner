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
BEGIN
    SELECT SUBSTRING(tablename, POSITION('.' in tablename)+1) INTO tablename_wo_schema;
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
    SELECT STRING_agg(cn.column_name, ',') INTO columnnames FROM (SELECT column_name FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;
    SELECT STRING_agg(cn.column, ',') INTO old_columnnames FROM (SELECT CONCAT('OLD.',column_name) as column FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;

-- Delete Rule
    EXECUTE format('CREATE RULE delete_from_%3$s AS ON DELETE TO %1$s ' ||
                   'DO ALSO (' ||
                   '    UPDATE hist.%3$s_hist SET valid_to = now() where %2$s;' ||
                   ');', tablename,primary_key_conditions, tablename_wo_schema);

    -- Update Rule
    EXECUTE format('CREATE RULE update_from_%3$s AS ON UPDATE TO %1$s ' ||
                   'DO ALSO (' ||
                   '    UPDATE hist.%3$s_hist SET valid_to = now() where %2$s;' ||
                   '    INSERT INTO hist.%3$s_hist SELECT * FROM %1$s WHERE %2$s;' ||
                   ');', tablename,primary_key_conditions, tablename_wo_schema);

    -- Insert Rule
    EXECUTE format('CREATE RULE insert_from_%3$s AS ON INSERT TO %1$s ' ||
                   'DO ALSO (' ||
                   '    INSERT INTO hist.%3$s_hist SELECT * FROM %1$s WHERE %2$s ' ||
                   ');', tablename, primary_key_conditions_new, tablename_wo_schema);


    EXECUTE format('INSERT INTO hist.versioned_tables(name) VALUES(''%s'');', tablename);
END
$$;

--CALL add_versioning_separated('data.station_image');



