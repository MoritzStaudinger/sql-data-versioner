DROP procedure if exists execute_queries();
CREATE OR REPLACE procedure execute_queries()
LANGUAGE plpgsql
AS
    $$
    DECLARE
        networks varchar[];
        stations integer[];
        s integer;
        t integer;
        n varchar;
        count integer = 0;
        que decimal[];
        timeseries integer[];

        download integer;
        original_query varchar;
        query varchar;
        tz timestamp;
        results integer;
    BEGIN
        RAISE NOTICE 'start';
        --SELECT array['FMI'] INTO networks; --from data.network;
        SELECT array['ARM', 'FMI',  'KIHS_CMC',  'KIHS_SMC',  'RSMN',  'SCAN',  'SNOTEL',  'TAHMO',  'USCRN',  'WEGENERNET'] INTO networks; --from data.network;
        RAISE NOTICE 'networks: %', array_length(networks,1);
        SELECT now()::timestamp into tz;
        SELECT save_download_simple(tz, 1) INTO download;
        FOREACH n IN ARRAY networks LOOP --72 networks
            count = count +1;
            SELECT CONCAT('SELECT array_agg(s.station_id) FROM (SELECT station_id, network_abbr FROM data.station s WHERE s.network_abbr = ''',n,''';') INTO original_query;
            SELECT CONCAT('SELECT array_agg(results.station_id) FROM(SELECT s.station_id, s.station_name, s.network_abbr, round(CAST(ST_X(station_location) AS NUMERIC),5) as x, round(CAST(ST_Y(station_location) AS NUMERIC),5) as y, ST_Z(station_location) as z FROM (SELECT * FROM data.station WHERE valid_from < ''',tz,''') as s WHERE s.network_abbr = ''',n,''' UNION SELECT s.station_id, s.station_name, s.network_abbr, round(CAST(ST_X(station_location) AS NUMERIC),5), round(CAST(ST_Y(station_location) AS NUMERIC),5), ST_Z(station_location) FROM (SELECT * FROM hist.station_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''')) as s WHERE s.network_abbr = ''',n,''' ORDER BY station_id) results;') into query;

            SELECT array_agg(s.station_id) INTO stations FROM (SELECT station_id, network_abbr FROM data.station) as s WHERE s.network_abbr = n;

            SELECT array_length(stations, 1) INTO results;

            PERFORM save_query_simple(original_query, query, results, md5(stations::varchar), download);

            IF array_length(stations, 1) > 0 THEN
                FOREACH s IN ARRAY stations LOOP
                    SELECT  CONCAT('SELECT array_agg(distinct timeseries_id) FROM (SELECT timeseries_id, variable_id, station_id, quantity_source_id FROM data.timeseries) as dt INNER JOIN (SELECT variable_id, variable_type FROM data.variable) dv ON dt.variable_id = dv.variable_id INNER JOIN (SELECT depth_id FROM data.depth) dd ON  dd.depth_id = dd.depth_id INNER JOIN (SELECT sensor_id FROM data.sensor) ds ON ds.sensor_id =  ds.sensor_id WHERE dt.station_id =', s,' AND dv.variable_type <> ''static'' AND dt.quantity_source_id = 0;') INTO original_query;
                    SELECT array_agg(distinct timeseries_id) INTO timeseries FROM (SELECT timeseries_id, variable_id, station_id, quantity_source_id FROM data.timeseries) as dt INNER JOIN (SELECT variable_id, variable_type FROM data.variable ) dv ON dt.variable_id = dv.variable_id INNER JOIN (SELECT depth_id FROM data.depth ) dd ON  dd.depth_id = dd.depth_id INNER JOIN (SELECT sensor_id FROM data.sensor) ds ON ds.sensor_id =  ds.sensor_id WHERE dt.station_id = s AND dv.variable_type <> 'static' AND dt.quantity_source_id = 0;
                    SELECT  CONCAT('SELECT array_agg(distinct results.timeseries_id) FROM (SELECT distinct timeseries_id, dv.variable_abbr, dv.variable_id, dd.depth_id, round(dd.depth_from, 2) as depth_from, round(dd.depth_to,2) as depth_to, ds.sensor_id, ds.sensor_name FROM (SELECT * FROM data.timeseries WHERE valid_from < ''',tz,''') as dt INNER JOIN (SELECT variable_id, variable_type, variable_abbr FROM data.variable WHERE valid_from < ''',tz,''') dv ON dt.variable_id = dv.variable_id INNER JOIN (SELECT * FROM data.depth WHERE valid_from < ''',tz,''') dd ON  dd.depth_id = dd.depth_id INNER JOIN (SELECT sensor_id,sensor_name FROM data.sensor WHERE valid_from < ''',tz,''') ds ON ds.sensor_id =  dt.sensor_id WHERE dt.station_id = ',s,' AND dv.variable_type <> ''static'' AND dt.quantity_source_id = 0 UNION SELECT distinct timeseries_id, dv.variable_abbr, dv.variable_id, dd.depth_id, round(dd.depth_from, 2), round(dd.depth_to,2), ds.sensor_id, ds.sensor_name  FROM (SELECT * FROM hist.timeseries_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''')) as dt INNER JOIN (SELECT variable_id, variable_type, variable_abbr FROM hist.variable_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''')) dv ON dt.variable_id = dv.variable_id INNER JOIN (SELECT * FROM hist.depth_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''') ) dd ON  dd.depth_id = dd.depth_id INNER JOIN (SELECT sensor_id,sensor_name FROM hist.sensor_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''')) ds ON dt.sensor_id =  ds.sensor_id WHERE dt.station_id =''',s,''' AND dv.variable_type <> ''static'' AND dt.quantity_source_id = 0) results ORDER BY timeseries_id, sensor_id, depth_id') INTO query;
                    SELECT array_length(timeseries, 1) INTO results;
                    PERFORM save_query_simple(original_query, query, results, md5(timeseries::varchar), download);
                    IF timeseries IS NOT NULL THEN
                    FOREACH t IN ARRAY timeseries  LOOP
                        count = count +1;

                        SELECT array_agg(round(a.dataset_value::decimal,2)) FROM (
                        SELECT to_char(dd.dataset_utc, 'YYYY-MM-DD HH24:MI'), dd.dataset_value,
                        (SELECT COALESCE(string_agg(df.flag_name, ','), 'M') AS dataset_qflag FROM unnest(dataset_qflag) dqflag LEFT JOIN (SELECT * FROM data.flag) df ON df.flag_id = dqflag),
                        (SELECT COALESCE(string_agg(dof.orig_flag_name, ','), 'M') AS dataset_origflag FROM unnest(dd.dataset_origflag) dqflag LEFT JOIN (SELECT * FROM data.orig_flag ) dof ON dof.orig_flag_id = dqflag),
                        '{dd.depth_from}',
                        '{dd.depth_to}',
                        '{dd.sensor_name}'
                        INTO que
                        FROM (SELECT * FROM data.dataset WHERE timeseries_id = t ) dd WHERE dataset_value <> 0 ORDER BY dd.dataset_id) a;

                        SELECT CONCAT('SELECT array_agg(round(a.dataset_value::decimal,2)) FROM (SELECT to_char(dd.dataset_utc, ''YYYY-MM-DD HH24:MI''), dd.dataset_value,
                        (SELECT COALESCE(string_agg(df.flag_name, '',''), ''M'') AS dataset_qflag FROM unnest(dataset_qflag) dqflag LEFT JOIN (SELECT * FROM data.flag) df ON df.flag_id = dqflag),
                        (SELECT COALESCE(string_agg(dof.orig_flag_name, '',''), ''M'') AS dataset_origflag FROM unnest(dd.dataset_origflag) dqflag LEFT JOIN (SELECT * FROM data.orig_flag) dof ON dof.orig_flag_id = dqflag),
                        ''{dd.depth_from}'',
                        ''{dd.depth_to}'',
                        ''{dd.sensor_name}''
                        FROM (SELECT * FROM data.dataset WHERE timeseries_id = ''',t,''') dd WHERE dd.dataset_value <> 0) a;') INTO original_query;

                        SELECT CONCAT('SELECT array_agg(round(results.dataset_value::decimal,2)) FROM (SELECT dd.dataset_id, to_char(dd.dataset_utc, ''YYYY-MM-DD HH24:MI'') as date, dd.dataset_value,(SELECT COALESCE(string_agg(df.flag_name, '',''), ''M'') AS dataset_qflag FROM unnest(dataset_qflag) dqflag LEFT JOIN (SELECT * FROM data.flag WHERE valid_from < ''',tz,''') df ON df.flag_id = dqflag WHERE valid_from < ''',tz,'''),(SELECT COALESCE(string_agg(dof.orig_flag_name, '',''), ''M'') AS dataset_origflag FROM unnest(dd.dataset_origflag) dqflag LEFT JOIN (SELECT * FROM data.orig_flag WHERE valid_from < ''',tz,''' ) dof ON dof.orig_flag_id = dqflag WHERE valid_from < ''',tz,''') FROM (SELECT * FROM data.dataset WHERE timeseries_id = ',t,' AND valid_from < ''',tz,''') dd WHERE dataset_value <> 0 UNION SELECT dd.dataset_id, to_char(dd.dataset_utc, ''YYYY-MM-DD HH24:MI''), dd.dataset_value, (SELECT COALESCE(string_agg(df.flag_name, '',''), ''M'') AS dataset_qflag FROM unnest(dataset_qflag) dqflag LEFT JOIN (SELECT flag_id, flag_name FROM hist.flag_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''') UNION SELECT flag_id, flag_name FROM data.flag WHERE valid_from < ''',tz,''' ) df ON df.flag_id = dqflag),(SELECT COALESCE(string_agg(dof.orig_flag_name, '',''), ''M'') AS dataset_origflag FROM unnest(dd.dataset_origflag) dqflag LEFT JOIN (SELECT orig_flag_id, orig_flag_name FROM hist.orig_flag_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''') UNION SELECT orig_flag_id, orig_flag_name FROM data.orig_flag WHERE valid_from < ''',tz,''') dof ON dof.orig_flag_id = dqflag) FROM (SELECT * FROM hist.dataset_hist WHERE valid_from < ''',tz,''' AND (valid_to IS null OR valid_to > ''',tz,''') AND timeseries_id = ',t,') dd WHERE dd.dataset_value <> 0) results ') INTO query;
                        SELECT array_length(que, 1) INTO results;
                        PERFORM save_query_simple(original_query, query, results, md5(que::varchar), download);
                    END LOOP;
                    END IF;
                END LOOP;
            END IF;
        END LOOP;
         RAISE NOTICE '%', count;
    END
    $$;
end;


