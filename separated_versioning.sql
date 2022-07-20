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
BEGIN
        -- Create Hybrid/Separated Table
    EXECUTE format('CREATE TABLE %s_hist(like %s);', tablename, tablename);
    -- Add valid_from column
    EXECUTE format('ALTER TABLE %s_hist ADD valid_from Timestamp;', tablename);
    EXECUTE format('ALTER TABLE %s_hist ALTER COLUMN valid_from SET default now();', tablename);
    -- Does NOW() behave correctly, otherwise replace with trigger
    EXECUTE format('INSERT INTO %s_hist SELECT * FROM %s;', tablename, tablename);
    EXECUTE format('ALTER TABLE %s_hist ADD valid_to Timestamp;', tablename);
    -- Change Primary Key for History table
    SELECT STRING_agg(column_name, ',')INTO primary_key FROM primary_keys WHERE table_name like tablename;
    SELECT STRING_agg(cn.pk, ' AND ')INTO primary_key_conditions FROM (SELECT CONCAT(column_name, '=OLD.',column_name)as pk FROM primary_keys WHERE table_name like tablename) as cn;
    SELECT STRING_agg(cn.pk, ' AND ')INTO primary_key_conditions_new FROM (SELECT CONCAT(column_name, '=NEW.',column_name)as pk FROM primary_keys WHERE table_name like tablename) as cn;
    EXECUTE format('ALTER TABLE %1$s_hist ADD CONSTRAINT %1$s_hist_pkey primary key(%2$s,valid_from); ', tablename,primary_key);
    -- Create necessary triggers on insert, update and delete
    -- Extract Columnames
    SELECT STRING_agg(cn.column_name, ',') INTO columnnames FROM (SELECT column_name FROM information_schema.columns WHERE table_name like tablename AND column_name <> 'valid_to' ORDER BY column_Name) as cn;
    SELECT STRING_agg(cn.column, ',') INTO old_columnnames FROM (SELECT CONCAT('OLD.',column_name) as column FROM information_schema.columns WHERE table_name like tablename AND column_name <> 'valid_to' ORDER BY column_Name) as cn;

-- Delete Rule
    EXECUTE format('CREATE RULE delete_from_%1$s AS ON DELETE TO %1$s ' ||
                   'DO ALSO (' ||
                   '    UPDATE %1$s_hist SET valid_to = now() where %2$s;' ||
                   ');', tablename,primary_key_conditions);

    -- Update Rule
    EXECUTE format('CREATE RULE update_from_%1$s AS ON UPDATE TO %1$s ' ||
                   'DO ALSO (' ||
                   '    UPDATE %1$s_hist SET valid_to = now() where %2$s;' ||
                   '    INSERT INTO %1$s_hist SELECT * FROM %1$s WHERE %2$s;' ||
                   ');', tablename,primary_key_conditions);

    -- Insert Rule
    EXECUTE format('CREATE RULE insert_from_%1$s AS ON INSERT TO %1$s ' ||
                   'DO ALSO (' ||
                   '    INSERT INTO %1$s_hist SELECT * FROM %1$s WHERE %2$s ' ||
                   ');', tablename, primary_key_conditions_new);


    EXECUTE format('INSERT INTO versioned_tables(name) VALUES(''%s'');', tablename);
END
$$;