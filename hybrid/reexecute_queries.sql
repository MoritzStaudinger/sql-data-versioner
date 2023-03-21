DROP function if exists reexecute_queries;
CREATE OR REPLACE function reexecute_queries(download integer)
RETURNS integer
LANGUAGE plpgsql
AS
    $$
    DECLARE
        queries varchar[];
        q record;
        results integer;
        que decimal[];
        timeseries integer[];
        stations integer[];
    BEGIN
            --SELECT array_agg(normalized_query) into queries FROM public.query_simple WHERE d_id = download;
            FOR q IN SELECT re_execute_query, result_hash, result_nr FROM public.query_simple WHERE d_id = download
            LOOP
                --RAISE NOTICE '%', q.re_execute_query;
                IF q.re_execute_query like '%array_agg(s.station_id)%' then
                    EXECUTE (q.re_execute_query) INTO stations;
                    IF array_length(stations, 1) <> q.result_nr then
                        --RAISE NOTICE 'query: %', q.re_execute_query;
                        RAISE NOTICE '%, %, %, %', md5(stations::varchar), q.result_hash, array_length(stations,1), q.result_nr;
                    end if;
                end if;
                IF q.re_execute_query like '%array_agg(distinct timeseries_id)%' then
                    EXECUTE (q.re_execute_query) INTO timeseries;
                    IF array_length(timeseries, 1) <> q.result_nr then
                        --RAISE NOTICE 'query: %', q.re_execute_query;
                        RAISE NOTICE '%, %, %, %', md5(timeseries::varchar), q.result_hash, array_length(timeseries,1), q.result_nr;
                    end if;
                end if;
                IF q.re_execute_query like '%to_char(dd%' then
                    EXECUTE (q.re_execute_query) INTO que;
                    IF array_length(que,1) <> q.result_nr then
                        --RAISE NOTICE 'query: %', q.re_execute_query;
                        RAISE NOTICE '%, %, %, %', md5(que::varchar), q.result_hash, array_length(que,1), q.result_nr;
                    end if;
                end if;
            END LOOP;
            RETURN 1;
    END
    $$;
end;

SELECT reexecute_queries(1);

select * from download_simple;
