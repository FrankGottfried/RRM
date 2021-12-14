# RRM

SQL code to be executed in the DB

Create a DB user called ACCESS_GRANTOR

```
CREATE USER ACCESS_GRANTOR PASSWORD "Abcd123!" NO FORCE_FIRST_PASSWORD_CHANGE;

-- create role
CREATE ROLE "data::external_access_g";
CREATE ROLE "data::external_access";

GRANT "data::external_access_g", "data::external_access" TO ACCESS_GRANTOR WITH ADMIN OPTION; 

GRANT AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION, AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION to "data::external_access_g";
GRANT AFL__SYS_AFL_AFLPAL_EXECUTE, AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION to "data::external_access" ;
```

Create a user-provided service call CC_ACCESS and provied the following json as credentials:
```
{
    "password": "Abcd123!",
    "schema": "ACCESS_GRANTOR",
    "tags": [
        "hana"
    ],
    "user": "ACCESS_GRANTOR"
}

```



SQL code to be executed inside the HDI container

```
-- create results table 
DROP TABLE PAL_AUTOARIMA_MODEL_TBL;  -- for the forecast followed
CREATE COLUMN TABLE PAL_AUTOARIMA_MODEL_TBL ("KEY" NVARCHAR(100), "VALUE" NVARCHAR(5000));

DROP TABLE PAL_AUTOARIMA_FIT_TBL;  -- for the forecast followed
CREATE COLUMN TABLE PAL_AUTOARIMA_FIT_TBL ("TIMESTAMP" INTEGER, "FITTED" DOUBLE, "RESIDUALS" double);

-- training 
DO
BEGIN
DECLARE param_name VARCHAR(5000) ARRAY;
DECLARE int_value INTEGER ARRAY;
DECLARE double_value DOUBLE ARRAY;
DECLARE string_value VARCHAR(5000) ARRAY;
param_name[1] := N'SEARCH_STRATEGY';
int_value[1] := 1;
double_value[1] := NULL;
string_value[1] := NULL;
param_name[2] := N'MAX_ORDER';
int_value[2] := 1;
double_value[2] := NULL;
string_value[2] := NULL;
param_name[3] := N'ALLOW_LINEAR';
int_value[3] := 1;
double_value[3] := NULL;
string_value[3] := NULL;
param_name[4] := N'OUTPUT_FITTED';
int_value[4] := 1;
double_value[4] := NULL;
string_value[4] := NULL;
param_name[5] := N'THREAD_RATIO';
int_value[5] := NULL;
double_value[5] := 1.0;
string_value[5] := NULL;
param_name[6] := N'DEPENDENT_VARIABLE';
int_value[6] := NULL;
double_value[6] := NULL;
string_value[6] := N'Y';
params = UNNEST(:param_name, :int_value, :double_value, :string_value);
in_0 = SELECT "TIMESTAMP", "Y" FROM "RPM_1"."TIMESERIES_HIST";
CALL "pal::autoarima"(:in_0, :params, out_0, out_1);

truncate table PAL_AUTOARIMA_MODEL_TBL;
insert into PAL_AUTOARIMA_MODEL_TBL (SELECT * FROM :out_0);
truncate table PAL_AUTOARIMA_FIT_TBL;
insert into PAL_AUTOARIMA_FIT_TBL (SELECT * FROM :out_1);

END;


-- prediction
DO
(IN in_0 TABLE ("TIMESTAMP" INT, "Y" DOUBLE) => "RPM_1"."TIMESERIES_TEST",
 IN in_1 TABLE ("KEY" NVARCHAR(100), "VALUE" NVARCHAR(5000)) => "PAL_AUTOARIMA_MODEL_TBL")
 
BEGIN
DECLARE param_name VARCHAR(5000) ARRAY;
DECLARE int_value INTEGER ARRAY;
DECLARE double_value DOUBLE ARRAY;
DECLARE string_value VARCHAR(5000) ARRAY;
param_name[1] := N'FORECAST_LENGTH';
int_value[1] := 10;
double_value[1] := NULL;
string_value[1] := NULL;
params = UNNEST(:param_name, :int_value, :double_value, :string_value);
CALL "pal::arima_forecast"(:in_0, :in_1, :params, out_0);
CREATE LOCAL TEMPORARY COLUMN TABLE "#PAL_ARIMA_FORECAST_RESULT_TBL" AS (SELECT * FROM :out_0);
END;


select * from #PAL_ARIMA_FORECAST_RESULT_TBL;


```
