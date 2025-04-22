# sfguide-geospatial-analysis-precisely-using-snowflake-notebooks-and-streamlit

## Overview 

Analyzing location-specific data for decision-making often requires specialized techniques typically handled by geography experts. However, with Snowflake, anyone can analyze geographic data—whether it's regional demographics, city points, market data, or points of interest—using the platform's built-in geospatial functionalities.


## Step-by-Step Guide

For prerequisites, environment setup, step-by-step guide and instructions, please refer to the [QuickStart Guide]([https://quickstarts.snowflake.com/guide/integrate_snowflake_cortex_agents_with_microsoft_teams/index.html](https://quickstarts.snowflake.com/guide/using_precisely_enrich_data/index.html?index=..%2F..index#0)).

**Steps for preparing data (Optional)**

--Preview some sample records from boundary table
select * from MBI__PREMIUM_GEOSPATIAL__MARKET_DATA.PROMOTIONAL."mbi_jp_cho_cho_moku_sample_region" limit 10;

--Preview some sample records from mbi market data
select * from MBI__PREMIUM_GEOSPATIAL__MARKET_DATA.PROMOTIONAL."mbi_marketdata_jp_cho_cho_moku_sample" limit 10;

--Preview some sample records from the table which is the result of joining the the market data and region data of MBI
select * from MBI__PREMIUM_GEOSPATIAL__MARKET_DATA.PROMOTIONAL."mbi_demographics_jp" limit 10;

--Preview some sample records from poi data which has is the result of extracting the poi data for the MBI region
select * from MBI__PREMIUM_GEOSPATIAL__MARKET_DATA.PROMOTIONAL."poi_jp" limit 10;

--create a new table by joining the boundary and market data using join on MICROCODE
create table "mbi_demographics_jp" as
select rgn.WKT, rgn.CTRYCODE, rgn.MICROCODE, rgn.NAME, rgn.GEOM,
mrktdata.P_T, mrktdata.P_PRM, mrktdata.HH_T, mrktdata.HH_SIZE, mrktdata.HH_I1, mrktdata.HH_I2, mrktdata.HH_I3, mrktdata.HH_I4, mrktdata.HH_I5, mrktdata.HHT_1, mrktdata.--HHT_2, mrktdata.HHT_3, mrktdata.HHT_4, mrktdata.MALE, mrktdata.FEMALE, mrktdata.AGE_T0014, mrktdata.AGE_M0014, mrktdata.AGE_F0014, mrktdata.AGE_T1529, mrktdata.--AGE_M1529, mrktdata.AGE_F1529, mrktdata.AGE_T3044, mrktdata.AGE_M3044, mrktdata.AGE_F3044, mrktdata.AGE_T4559, mrktdata.AGE_M4559, mrktdata.AGE_F4559, mrktdata.--AGE_T60PL, mrktdata.AGE_M60PL, mrktdata.AGE_F60PL, mrktdata.EDU_1, mrktdata.EDU_2, mrktdata.EDU_3, mrktdata.EDU_4, mrktdata.EDU_5, mrktdata.EDU_6, mrktdata.EDU_7, --mrktdata.EDU_8, mrktdata.MA_1, mrktdata.MA_2, mrktdata.MA_3, mrktdata.MA_4, mrktdata.UNEMPL, mrktdata.PP_MIO, mrktdata.PP_PRM, mrktdata.PP_EURO, mrktdata.PP_CI, --mrktdata.CSP01_MIO, mrktdata.CSP01_PRM, mrktdata.CSP01_EURO, mrktdata.CSP01_CI, mrktdata.CSP02_MIO, mrktdata.CSP02_PRM, mrktdata.CSP02_EURO, mrktdata.CSP02_CI, mrktdata.--CSP03_MIO, mrktdata.CSP03_PRM, mrktdata.CSP03_EURO, mrktdata.CSP03_CI, mrktdata.CSP04_MIO, mrktdata.CSP04_PRM, mrktdata.CSP04_EURO, mrktdata.CSP04_CI, mrktdata.--CSP05_MIO, mrktdata.CSP05_PRM, mrktdata.CSP05_EURO, mrktdata.CSP05_CI, mrktdata.CSP06_MIO, mrktdata.CSP06_PRM, mrktdata.CSP06_EURO, mrktdata.CSP06_CI, mrktdata.--CSP07_MIO, mrktdata.CSP07_PRM, mrktdata.CSP07_EURO, mrktdata.CSP07_CI, mrktdata.CSP08_MIO, mrktdata.CSP08_PRM, mrktdata.CSP08_EURO, mrktdata.CSP08_CI, mrktdata.--CSP09_MIO, mrktdata.CSP09_PRM, mrktdata.CSP09_EURO, mrktdata.CSP09_CI, mrktdata.CSP10_MIO, mrktdata.CSP10_PRM, mrktdata.CSP10_EURO, mrktdata.CSP10_CI, mrktdata.--CSP11_MIO, mrktdata.CSP11_PRM, mrktdata.CSP11_EURO, mrktdata.CSP11_CI, mrktdata.CSP12_MIO, mrktdata.CSP12_PRM, mrktdata.CSP12_EURO, mrktdata.CSP12_CI, mrktdata.--CSP13_MIO, mrktdata.CSP13_PRM, mrktdata.CSP13_EURO, mrktdata.CSP13_CI, mrktdata.CSP14_MIO, mrktdata.CSP14_PRM, mrktdata.CSP14_EURO, mrktdata.CSP14_CI, mrktdata.--CSP15_MIO, mrktdata.CSP15_PRM, mrktdata.CSP15_EURO, mrktdata.CSP15_CI, mrktdata.CSP16_MIO, mrktdata.CSP16_PRM, mrktdata.CSP16_EURO, mrktdata.CSP16_CI, mrktdata.--CSP17_MIO, mrktdata.CSP17_PRM, mrktdata.CSP17_EURO, mrktdata.CSP17_CI, mrktdata.CSP18_MIO, mrktdata.CSP18_PRM, mrktdata.CSP18_EURO, mrktdata.CSP18_CI, mrktdata.--CSP19_MIO, mrktdata.CSP19_PRM, mrktdata.CSP19_EURO, mrktdata.CSP19_CI, mrktdata.CSP20_MIO, mrktdata.CSP20_PRM, mrktdata.CSP20_EURO, mrktdata.CSP20_CI, mrktdata.P_T_TRE, --mrktdata.HH_T_TRE, mrktdata.MALE_TRE, mrktdata.FEMALE_TRE, mrktdata.T0014_TRE, mrktdata.M0014_TRE, mrktdata.F0014_TRE, mrktdata.T1529_TRE, mrktdata.M1529_TRE, mrktdata.--F1529_TRE, mrktdata.T3044_TRE, mrktdata.M3044_TRE, mrktdata.F3044_TRE, mrktdata.T4559_TRE, mrktdata.M4559_TRE, mrktdata.F4559_TRE, mrktdata.T60PL_TRE, mrktdata.--M60PL_TRE, mrktdata.F60PL_TRE, mrktdata.ALO_T_TRE, mrktdata.PP_CI_TRE, mrktdata.CSP01_TRE, mrktdata.CSP02_TRE, mrktdata.CSP03_TRE, mrktdata.CSP04_TRE, mrktdata.--CSP05_TRE, mrktdata.CSP06_TRE, mrktdata.CSP07_TRE, mrktdata.CSP08_TRE, mrktdata.CSP09_TRE, mrktdata.CSP10_TRE, mrktdata.CSP11_TRE, mrktdata.CSP12_TRE, mrktdata.--CSP13_TRE, mrktdata.CSP14_TRE, mrktdata.CSP15_TRE, mrktdata.CSP16_TRE, mrktdata.CSP17_TRE, mrktdata.CSP18_TRE, mrktdata.CSP19_TRE, mrktdata.CSP20_TRE
from
MBI__PREMIUM_GEOSPATIAL__MARKET_DATA.PROMOTIONAL."mbi_jp_cho_cho_moku_sample_region" rgn join MBI__PREMIUM_GEOSPATIAL__MARKET_DATA.PROMOTIONAL.--"mbi_marketdata_jp_cho_cho_moku_sample" mrktdata
on(rgn.MICROCODE = mrktdata.MICROCODE);

Set SRID for compatibility on spatial operations on both the table.
UPDATE "mbi_demographics_jp"
SET GEOM = ST_SetSRID(TRY_TO_GEOMETRY(WKT), 4326);

use the ST_Contains spatial function for getting all the points of interest data for this region and storing the results in a new table
create table "poi_jp" as
SELECT
     poi.*
FROM "mbi_demographics_jp" region
JOIN "poi_jp_cho_cho_moku" poi
ON ST_Contains(region.GEOM, poi.GEOM);
