import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.*;
import java.io.IOException;


public class Weather_Avro {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Avro Weather");
        job.setJarByClass(Weather_Avro.class);

        Path inputFile = new Path(args[0]);
        Path outputAvroFile = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);

        Schema schema = Weather.getClassSchema();

//        AvroWriter.Writer(conf, schema,fs,inputFile, outputAvroFile);
//        AvroReader.Reader(schema, outputAvroFile, conf,fs);

        FileInputFormat.setInputPaths(job,outputAvroFile);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(WeatherMapper.class);
        AvroJob.setInputKeySchema(job,Weather.getClassSchema());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(WeatherPartitioner.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(org.apache.avro.Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job,Schema.create(org.apache.avro.Schema.Type.FLOAT));



        System.exit(job.waitForCompletion(true) ? 0:1);


    }


}
