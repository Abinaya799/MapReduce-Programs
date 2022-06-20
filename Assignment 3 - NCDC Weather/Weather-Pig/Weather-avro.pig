SET mapreduce.map.output.compress true;
SET mapreduce.output.compress true;
SET mapreduce.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;


REGISTER /usr/lib/pig/piggybank.jar;
REGISTER /usr/lib/pig/lib/avro-*.jar;
REGISTER /usr/lib/pig/lib/jackson-core-asl-*.jar;
REGISTER /usr/lib/pig/lib/jackson-mapper-asl-*.jar;
REGISTER /usr/lib/pig/lib/json-simple-*.jar;


data = LOAD '2013/*' USING PigStorage(',') as (STATION:chararray,DATE:chararray,LATITUDE:chararray,LONGITUDE:chararray,ELEVATION:chararray,NAME:chararray,namei:chararray,TEMP:chararray,TEMP_ATTRIBUTES:chararray,DEWP:chararray,DEWP_ATTRIBUTES:chararray,SLP:chararray,SLP_ATTRIBUTES:chararray,STP:chararray,STP_ATTRIBUTES:chararray,VISIB:chararray,VISIB_ATTRIBUTES:chararray,WDSP:chararray,WDSP_ATTRIBUTES:chararray,MXSPD:chararray,GUST:chararray,MAX:chararray,MAX_ATTRIBUTES:chararray,MIN:chararray,MIN_ATTRIBUTES:chararray,PRCP:chararray,PRCP_ATTRIBUTES:chararray,SNDP:chararray,FRSHTT:chararray);
data_without_header = filter data by (STATION !='"STATION"');
clean_data = foreach data_without_header generate REPLACE(STATION,'"','')as station,REPLACE(DATE,'"','') as date,(float) (REPLACE(MAX,'"','')) as max, (float)(REPLACE(MIN,'"','')) as min;
difference = foreach clean_data generate station,max-min as diff;
average = foreach (GROUP difference by station) generate group as station, (float) AVG(difference.diff) as average;

STORE average INTO 'pig-avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
        '{
            "schema": {
				"namespace": "",
				 "type": "record",
				 "name": "Weather",
				 "fields": [
					 {"name": "station", "type": "string"},
					 {"name": "Average", "type": ["float","null"]}
				 ]
            }
        }');

stocks_avro = LOAD 'pig-avro/part-r-00000'
          USING org.apache.pig.piggybank.storage.avro.AvroStorage(
		  'no_schema_check',
                          'schema_file', 
		  '/user/hirwuser2733/Weather.avro.schema');
		  



