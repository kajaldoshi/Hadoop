create database if not exists crimedb;

use crimedb;

create external table if not exists crimedata ( id int, case_number String, date String, block String, iucr String, primary_type String, desc String, loc_desc String, arrest String, domestic String, beat int, district int, ward int, community_area int, fbi_code String, x-coord int, y-coord int, year int, updated_on String, lat double, long double, loc struct<double,double> COMMENT 'Overall Crime Data' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/usr/CrimeData';


insert overwrite directory '/usr/MasterCrimeDataOutput/Domestic' row format delimited fields terminated by ',' 

select domestic, count(id) as cnt from crimedata where domestic in ('true','false') group by domestic;

 
