import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Weather_2 {
    enum Counters {
        NOT_GOOD_RECORDS, GOOD_RECORDS
    }

    public static class AMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public static boolean isNumeric(String strNum) {
            if (strNum == null) {
                return false;
            }
            try {
                Double.parseDouble(strNum);
            } catch (NumberFormatException nfe) {
                return false;
            }
            return true;
        }

        @Override
        public void map(LongWritable keyin, Text valuein, Context context) throws IOException, InterruptedException {
            String lines = valuein.toString();
//            Ignore the column header
            String[] fields = lines.replaceAll("\"", "").split(",");
            String min = fields[23].replaceAll(" ", "");
            String max = fields[21].replaceAll(" ", "");
            if (isNumeric(min) && isNumeric(max)) {
                String station = fields[0];
                float temp_min = Float.parseFloat(min);
                float temp_max = Float.parseFloat(max);
                float diff = temp_max - temp_min;
                context.getCounter(Counters.GOOD_RECORDS).increment(1);
//                String keyout = station + "," + date;
                context.write(new Text(station), new FloatWritable(diff));
            }
            else{
                context.getCounter(Counters.NOT_GOOD_RECORDS).increment(1);
            }

        }

    }

    public static class AReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
        @Override
        public void reduce(Text keyIn,Iterable<FloatWritable> valueIn,Context context) throws IOException, InterruptedException {
            int count = 0;
            float sum =0;
            while (valueIn.iterator().hasNext()){
                count++;
                sum+=valueIn.iterator().next().get();
            }
            FloatWritable average = new FloatWritable(sum/count);
            context.write(keyIn,average);
        }
    }

    public static class APartitioner extends Partitioner<Text,FloatWritable> {

        @Override
        public int getPartition(Text text, FloatWritable floatWritable, int numPartitions) {
            if(numPartitions == 0)
            {
                return 0;
            }
            String reduceCriterion = text.toString();
            if(reduceCriterion.matches("(012)[0-2]{1}\\d*")){
                return 0;
            }
            else if (reduceCriterion.matches("(012)[3-6]{1}\\d*")){
                return 1%numPartitions;
            }
            else{
               return 2%numPartitions; 
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job new_job = Job.getInstance(conf, "Assignment_2");
        new_job.setJarByClass(Weather_2.class);
        new_job.setMapperClass(AMapper.class);
        new_job.setPartitionerClass(APartitioner.class);
        new_job.setReducerClass(AReducer.class);
        new_job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(new_job, new Path(args[0]));
        FileOutputFormat.setOutputPath(new_job, new Path(args[1]));

        new_job.setInputFormatClass(SequenceFileInputFormat.class);
        new_job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setCompressOutput(new_job, true);
        FileOutputFormat.setOutputCompressorClass(new_job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(new_job, SequenceFile.CompressionType.BLOCK);

        new_job.setMapOutputKeyClass(Text.class);
        new_job.setMapOutputValueClass(FloatWritable.class);

        new_job.setOutputKeyClass(Text.class);
        new_job.setOutputValueClass(FloatWritable.class);
        
        System.exit(new_job.waitForCompletion(true) ? 0 : 1);


    }

    }


