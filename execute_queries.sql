DROP procedure if exists execute_queries();
CREATE OR REPLACE procedure execute_queries()
LANGUAGE plpgsql
AS
    $$
    DECLARE
        networks varchar[];
        stations integer[];
        s integer;
        n varchar;
        count integer = 0;
        que varchar;
        timeseries integer[];
    BEGIN
         RAISE NOTICE 'start';
        SELECT array_agg('ARM', 'FMI',  'KIHS_CMC',  'KIHS_SMC',  'RSMN',  'SCAN',  'SNOTEL'  'TAHMO'  'USCRN'  'WEGENERNET') INTO networks from data.network;
        FOREACH n IN ARRAY networks LOOP --72 networks
            count = count +1;
            SELECT array_agg(station_id) INTO stations FROM data.station WHERE network_abbr = n;
            IF array_length(stations, 1) > 0 THEN
            FOREACH s IN ARRAY stations LOOP
            SELECT array_agg(timeseries_id) INTO timeseries FROM data.timeseries  INNER JOIN data.variable ON timeseries.variable_id = variable.variable_id INNER JOIN data.depth ON  timeseries.depth_id = depth.depth_id INNER JOIN data.sensor ON timeseries.sensor_id =  sensor.sensor_id
            WHERE station_id = s AND variable.variable_type <> 'static' AND quantity_source_id = 0;
            IF timeseries IS NOT NULL THEN
            FOREACH s IN ARRAY timeseries  LOOP
                count = count +1;
                SELECT to_char(dataset.dataset_utc, 'YYYY-MM-DD HH24:MI'),
                dataset.dataset_value,
                (SELECT COALESCE(string_agg(flag.flag_name, ','), 'M') AS dataset_qflag FROM unnest(dataset_qflag) dqflag LEFT JOIN data.flag ON data.flag.flag_id = dqflag),
                (SELECT COALESCE(string_agg(orig_flag.orig_flag_name, ','), 'M') AS dataset_origflag FROM unnest(dataset_origflag) dqflag LEFT JOIN data.orig_flag ON data.orig_flag.orig_flag_id = dqflag),
                '{depth_from}' AS depth_from,
                '{depth_to}' AS depth_to,
                '{sensor_name}' AS sensor_name
                INTO que
                FROM data.dataset
                WHERE timeseries_id = s;
            END LOOP;
            END IF;
        END LOOP;
            END IF;
        END LOOP;
         RAISE NOTICE '%', count;
    END
    $$;
end;

