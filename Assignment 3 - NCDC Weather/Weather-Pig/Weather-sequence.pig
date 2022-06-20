SET mapreduce.map.output.compress true;
SET mapreduce.output.compress true;
SET mapreduce.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

REGISTER 'elephant-bird/elephant-bird-core-4.0.jar';
REGISTER 'elephant-bird/elephant-bird-hadoop-compat-4.0.jar';
REGISTER 'elephant-bird/elephant-bird-pig-4.0.jar';
REGISTER 'elephant-bird/piggybank.jar';

DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

data = LOAD '2013/*' USING PigStorage(',') as (STATION:chararray,DATE:chararray,LATITUDE:chararray,LONGITUDE:chararray,ELEVATION:chararray,NAME:chararray,namei:chararray,TEMP:chararray,TEMP_ATTRIBUTES:chararray,DEWP:chararray,DEWP_ATTRIBUTES:chararray,SLP:chararray,SLP_ATTRIBUTES:chararray,STP:chararray,STP_ATTRIBUTES:chararray,VISIB:chararray,VISIB_ATTRIBUTES:chararray,WDSP:chararray,WDSP_ATTRIBUTES:chararray,MXSPD:chararray,GUST:chararray,MAX:chararray,MAX_ATTRIBUTES:chararray,MIN:chararray,MIN_ATTRIBUTES:chararray,PRCP:chararray,PRCP_ATTRIBUTES:chararray,SNDP:chararray,FRSHTT:chararray);
data_without_header = filter data by (STATION !='"STATION"');
clean_data = foreach data_without_header generate REPLACE(STATION,'"','')as station,REPLACE(DATE,'"','') as date,(float) (REPLACE(MAX,'"','')) as max, (float)(REPLACE(MIN,'"','')) as min;
difference = foreach clean_data generate station,max-min as diff;
average = foreach (GROUP difference by station) generate group as station, AVG(difference.diff) as average;

STORE average INTO '/user/hirwuser2733/pig-sequence' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
  '-c com.twitter.elephantbird.pig.util.TextConverter', 
  '-c com.twitter.elephantbird.pig.util.TextConverter'
);

hdfs dfs -copyToLocal /user/hirwuser2733/pig-sequence .

read_seq = LOAD 'pig-sequence/part-r-00000' USING SequenceFileLoader as (STATION:chararray, Average:float);
DUMP read_seq;





