import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<AvroKey<Weather>, NullWritable, Text, FloatWritable> {
    @Override
    public void map(AvroKey<Weather> keyIn, NullWritable value, Context context) throws IOException,InterruptedException{
        String station = keyIn.datum().getStation().toString();
        Float max_temp = keyIn.datum().getMax();
        Float min_temp = keyIn.datum().getMin();
        float diff = max_temp-min_temp;
        context.write(new Text(station),new FloatWritable(diff));
    }

}
