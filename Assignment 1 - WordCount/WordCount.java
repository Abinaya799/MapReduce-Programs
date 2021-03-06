import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCount{

    # For getting the number of words in the data file as a counter in the Mapreduce result
    enum Example{
        NUMBER_OF_WORDS
    }
    
    #Mapper class
    public static class AMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        public void map(LongWritable keyin,Text valuein,Context context) throws IOException, InterruptedException {
            String line = valuein.toString();
            String[] words = line.split(" ");
            for (String word:
                 words) {
                if(!word.equals(" ") & word.matches("[a-zA-Z]+")) {
word = word.replaceAll("[^a-zA-Z]","");                    context.getCounter(Example.NUMBER_OF_WORDS).increment(1);
                    context.write(new Text(word), new IntWritable(1));
                }
            }

        }

    }
    
    # Reducer class
    public static class AReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
        @Override
        public void reduce(Text keyin, Iterable<IntWritable> valuein, Context context) throws IOException, InterruptedException {
            int count = 0;
            while(valuein.iterator().hasNext()){
                count += valuein.iterator().next().get();
            }
            context.write(keyin,new IntWritable(count));
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
 {
        # Assign the new mapreduce job
        Job new_job = new Job();
        new_job.setJobName("Assignment 1");
        new_job.setMapperClass(AMapper.class);
        new_job.setJarByClass(Assignment.class);
        new_job.setReducerClass(AReducer.class);

        #Specify the input data file location and the output directory .Make sure that output directory doesn't already exists
        FileInputFormat.addInputPath(new_job,new Path(args[0]));
        FileOutputFormat.setOutputPath(new_job,new Path(args[1]));

        #Specify the data format of input and output files
        new_job.setInputFormatClass(TextInputFormat.class);
        new_job.setOutputFormatClass(TextOutputFormat.class);

        #Specify the Wriatble type for the output key and value
        new_job.setOutputKeyClass(Text.class);
        new_job.setOutputValueClass(IntWritable.class);
        new_job.setMapOutputKeyClass(Text.class);
        new_job.setMapOutputValueClass(IntWritable.class);
        
        System.exit(new_job.waitForCompletion(true) ? 0 : 1);

    }
}
