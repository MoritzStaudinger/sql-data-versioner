DROP procedure if exists add_versioning_integrated;
CREATE OR REPLACE procedure add_versioning_integrated(tablename varchar)
language plpgsql
as $$
DECLARE
    primary_key  varchar;
    primary_key_conditions varchar;
    primary_key_conditions_new varchar;
    columnnames varchar;
    old_columnnames varchar;
BEGIN
    EXECUTE format('ALTER TABLE %s ADD valid_from Timestamp;', tablename);
    EXECUTE format('ALTER TABLE %s ALTER COLUMN valid_from SET default now();', tablename);
    -- Does NOW() behave correctly, otherwise replace with trigger
    EXECUTE format('UPDATE %s SET valid_from = NOW() WHERE valid_from is NULL;', tablename);
    EXECUTE format('ALTER TABLE %s ADD valid_to Timestamp;', tablename);
    -- extraction of primary keys, and add valid_from to the primary key
    SELECT STRING_agg(column_name, ',')INTO primary_key FROM primary_keys WHERE table_name like 'accounts';
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s_pkey;', tablename, tablename);
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_pkey primary key(%s,valid_from) ', tablename,tablename,primary_key);

    SELECT STRING_agg(cn.pk, ' AND ')INTO primary_key_conditions FROM (SELECT CONCAT(column_name, '=OLD.',column_name)as pk FROM primary_keys WHERE table_name like tablename AND column_name <> 'valid_to') as cn;
    SELECT STRING_agg(cn.pk, ' AND ')INTO primary_key_conditions_new FROM (SELECT CONCAT(column_name, '=NEW.',column_name)as pk FROM primary_keys WHERE table_name like tablename AND column_name <> 'valid_to') as cn;
    SELECT STRING_agg(cn.column_name, ',') INTO columnnames FROM (SELECT column_name FROM information_schema.columns WHERE table_name like tablename ORDER BY column_Name) as cn;
    SELECT STRING_agg(cn.column, ',') INTO old_columnnames FROM (SELECT CONCAT('OLD.',column_name) as column FROM information_schema.columns WHERE table_name like tablename AND column_name <> 'valid_to' AND column_name <> 'valid_from' ORDER BY column_Name) as cn;


    -- Delete Rule
    EXECUTE format('CREATE RULE delete_from_%1$s AS ON DELETE TO %1$s ' ||
                   'DO INSTEAD (' ||
                   '    UPDATE %1$s SET valid_to = now() where %2$s;' ||
                   ');', tablename,primary_key_conditions);

    -- Update Rule
    EXECUTE format('CREATE OR REPLACE FUNCTION  update_trigger_function_%1$s() ' ||
                   'RETURNS TRIGGER ' ||
                   'LANGUAGE PLPGSQL ' ||
                   'AS ' ||
                   '$update_trigger_function_%1$s$ BEGIN'
                   '    INSERT INTO %1$s(%2$s) VALUES(%3$s, OLD.valid_from - interval ''1 milliseconds'', now() );' ||
                   '    NEW.valid_to = now();' ||
                   '    RETURN NEW;' ||
                   'END; ' ||
                   '$update_trigger_function_%1$s$;', tablename, columnnames, old_columnnames);

    EXECUTE format('CREATE TRIGGER update_trigger_%1$s ' ||
                   'BEFORE UPDATE ' ||
                   'ON %1$s ' ||
                   'FOR EACH ROW ' ||
                   '    WHEN (pg_trigger_depth() < 1)' ||
                   '        EXECUTE PROCEDURE update_trigger_function_%1$s();', tablename);

    -- No insert rule needed, as done by default
END
$$;
