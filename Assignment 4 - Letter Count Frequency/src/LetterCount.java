import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import java.io.IOException;

public class LetterCount extends Configured implements Tool {

    public static Schema SCHEMA$ = LetterCountOutputAvro.getClassSchema();

    public static void deleteIfExists(Path path,Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)){
            fs.delete(path,true);
        }
    }

    public  static void main(String[] args) throws Exception {

        int exitFlag = ToolRunner.run( new LetterCount(), args);
        System.exit(exitFlag);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf,"Letter Count");
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job1,SCHEMA$);
        job1.setJarByClass(LetterCount.class);

        Path inputPath = new Path(args[0]);
        Path avroOutputPath = new Path(args[1]);
        Path MROutputPath = new Path(args[2]);

        deleteIfExists(avroOutputPath,conf);
        deleteIfExists(MROutputPath,conf);

        FileInputFormat.addInputPath(job1,inputPath);
        FileOutputFormat.setOutputPath(job1,avroOutputPath);

        job1.setMapperClass(LetterCountMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setReducerClass(LetterCountReducer.class);


        Job job2 = Job.getInstance(conf,"Letter Count");
        job2.setJarByClass(LetterCount.class);
        job2.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job2,SCHEMA$);
        job2.setOutputFormatClass(OrcOutputFormat.class);
        FileInputFormat.addInputPath(job2,avroOutputPath);
        FileOutputFormat.setOutputPath(job2,MROutputPath);
        job2.setMapperClass(LetterFrequencyMapper.class);
        job2.setMapOutputKeyClass(AvroKey.class);
        job2.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job2,Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job2,SCHEMA$);
        job2.setReducerClass(LetterFrequencyReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(OrcStruct.class);
        job2.setReducerClass(LetterFrequencyReducer.class);

        job1.waitForCompletion(true);
        job2.waitForCompletion(true);
        return 0;
    }
}

