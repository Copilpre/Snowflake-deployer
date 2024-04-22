CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='\"';

CREATE OR REPLACE PROCEDURE DB_SPRINT1.SCH_SPRINT1.LOAD_BRONZE_TABLE("TABLE_NAME" VARCHAR(16777216), "FILE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
--EXECUTE AS OWNER
AS $$
  declare
    location   varchar;
    copy_stmt  varchar;
  begin
    ----------------------
    -- log start
    ----------------------
    system$log_info('start');
    ----------------------
    -- Create the table from the infered schema
    ----------------------
    table_name := REPLACE(:table_name, '/', '_');
    table_name := REPLACE(:table_name, '.', '_');
    table_name := REPLACE(:table_name, '-', '_');
    system$log_info('create table ""' || :table_name || '"" from file ""'|| :file_name || '"');
    location := concat('@MY_AWS/', :file_name);
    create or replace table identifier(:table_name) using template (
        select array_agg(object_construct(*))
        from table(
            infer_schema(
                location=> :location,
                file_format=>'my_csv_format',
                IGNORE_CASE=>TRUE
            )
        )
    );
    ----------------------
    -- Load the data from the file to the target table
    ----------------------
    system$log_info('copy into table "' || :table_name || '" from file "'|| :file_name || '"');
    ----------------------
    -- log success
    ----------------------
    copy_stmt :=
        'COPY INTO ' || :table_name ||
        ' FROM @MY_AWS/'|| :file_name ||
        '   FILE_FORMAT = (FORMAT_NAME= ''my_csv_format'')
         MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE' ;
    execute immediate :copy_stmt;
    ----------------------
    -- return success
    ----------------------
    return 'success ' ||  :table_name;
  
  end
  $$;


CREATE OR REPLACE PROCEDURE creation_tables_cursor()
  RETURNS text
  LANGUAGE SQL
  AS
  $$
    -- Snowflake Scripting code
    DECLARE
    
    files_list RESULTSET DEFAULT (SELECT DISTINCT METADATA$FILENAME as filename from @MY_AWS);
    c1 CURSOR FOR files_list;
    
    BEGIN
        FOR record IN c1 DO
            
            let command varchar := 'call LOAD_BRONZE_TABLE(''' || record.filename || ''', ''' || record.filename || ''')';
            execute immediate command;
            
            END FOR;
            return 'done';
    END;
$$
;

call creation_tables_cursor();

SHOW TERSE TABLES IN DB_SPRINT1.SCH_SPRINT1;