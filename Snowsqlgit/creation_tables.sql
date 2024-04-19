CREATE OR REPLACE TABLE MY_TMP (
    id INTEGER AUTOINCREMENT,
    text TEXT
) AS 
SELECT ROW_NUMBER() OVER(ORDER BY METADATA$FILENAME) AS id, METADATA$FILENAME
FROM (SELECT DISTINCT METADATA$FILENAME FROM @MY_AWS);


CREATE OR REPLACE PROCEDURE DB_SPRINT1.SCH_SPRINT1.LOAD_BRONZE_TABLE_TEST("TABLE_NAME" VARCHAR(16777216), "FILE_NAME" VARCHAR(16777216))
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


CREATE OR REPLACE PROCEDURE creation_tables()
  RETURNS text
  LANGUAGE SQL
  AS
  $$
    -- Snowflake Scripting code
    DECLARE
    counter INTEGER DEFAULT 0;
    maximum_count INTEGER default (SELECT COUNT(text) from MY_TMP);
    file_name_var text ;
    suppression text;
    
    BEGIN
        FOR i IN 0 TO maximum_count DO
            counter := counter + 1;
            -----Select le nom du premier fichier présent dans la table my_tmp
            file_name_var := (SELECT text FROM MY_TMP ORDER BY ID LIMIT 1);
            -----Suppression de la ligne correspondant au premier fichier de la table
            suppression:= 'DELETE FROM MY_TMP WHERE MY_TMP.ID = ' || :counter;
            execute immediate suppression;
            ----création de la table
            let command varchar := 'call LOAD_BRONZE_TABLE_TEST(''' || :file_name_var || ''', ''' || :file_name_var || ''')';
            execute immediate command;
            
            END FOR;
            RETURN :counter;
    END;
$$
;

call creation_tables();

DROP TABLE MY_TMP;