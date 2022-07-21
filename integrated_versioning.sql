DROP procedure if exists add_versioning_integrated;
CREATE OR REPLACE procedure add_versioning_integrated(tablename varchar)
language plpgsql
as $$
DECLARE
    primary_key  varchar;
    primary_key_conditions varchar;
    primary_key_conditions_new varchar;
    columnnames varchar;
    new_columnnames varchar;
    column_reset varchar;
    tablename_wo_schema varchar;
BEGIN
    SELECT SUBSTRING(tablename, POSITION('.' in tablename)+1) INTO tablename_wo_schema;
    EXECUTE format('ALTER TABLE %s ADD valid_from Timestamp;', tablename);
    EXECUTE format('ALTER TABLE %s ALTER COLUMN valid_from SET default now();', tablename);
    -- Does NOW() behave correctly, otherwise replace with trigger
    EXECUTE format('UPDATE %s SET valid_from = NOW() WHERE valid_from is NULL;', tablename);
    EXECUTE format('ALTER TABLE %s ADD valid_to Timestamp;', tablename_wo_schema);
    -- extraction of primary keys, and add valid_from to the primary key
    SELECT STRING_agg(column_name, ',')INTO primary_key FROM primary_keys WHERE table_name like tablename_wo_schema;
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s_pkey;', tablename, tablename_wo_schema);
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_pkey primary key(%s,valid_from) ', tablename,tablename_wo_schema,primary_key);

    SELECT STRING_agg(cn.pk, ' AND ')INTO primary_key_conditions FROM (SELECT CONCAT(column_name, '=OLD.',column_name)as pk FROM primary_keys WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to') as cn;
    SELECT STRING_agg(cn.pk, ' AND ')INTO primary_key_conditions_new FROM (SELECT CONCAT(column_name, '=NEW.',column_name)as pk FROM primary_keys WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to') as cn;
    SELECT STRING_agg(cn.column_name, ',') INTO columnnames FROM (SELECT column_name FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' AND column_name <> 'valid_from' ORDER BY column_Name) as cn;
    SELECT STRING_agg(cn.column, ',') INTO new_columnnames FROM (SELECT CONCAT('NEW.',column_name) as column FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' AND column_name <> 'valid_from' ORDER BY column_Name) as cn;
    SELECT STRING_agg(concat(cn.ncolumn, '=', cn.ocolumn), ';') INTO column_reset FROM (SELECT CONCAT('OLD.',column_name) as ocolumn, CONCAT('NEW.', column_name) as ncolumn FROM information_schema.columns WHERE table_name like tablename_wo_schema AND column_name <> 'valid_to' ORDER BY column_Name) as cn;


    -- Delete Rule
    EXECUTE format('CREATE RULE delete_from_%3$s AS ON DELETE TO %1$s ' ||
                   'DO INSTEAD (' ||
                   '    UPDATE %1$s SET valid_to = now() where %2$s;' ||
                   ');', tablename,primary_key_conditions, tablename_wo_schema);

    -- Update Rule
    EXECUTE format('CREATE OR REPLACE FUNCTION  update_trigger_function_%3$s() ' ||
                   'RETURNS TRIGGER ' ||
                   'LANGUAGE PLPGSQL ' ||
                   'AS $update_trigger_function$ ' ||
                   'BEGIN' ||
                   '    IF new.valid_to IS NULL THEN ' ||
                   '    INSERT INTO %3$s(%1$s) VALUES(%2$s);' ||
                   '    %4$s;' ||
                   '    NEW.valid_to = now();' ||
                   '    END IF;' ||
                   '    RETURN NEW;' ||
                   'END; $update_trigger_function$ ', columnnames, new_columnnames, tablename_wo_schema,column_reset);

    EXECUTE format('CREATE TRIGGER update_trigger_%2$s ' ||
                   'BEFORE UPDATE ' ||
                   'ON %1$s ' ||
                   'FOR EACH ROW ' ||
                   '    WHEN (pg_trigger_depth() < 1)' ||
                   '        EXECUTE PROCEDURE update_trigger_function_%2$s();', tablename, tablename_wo_schema);

    -- No insert rule needed, as done by default
END
$$;
