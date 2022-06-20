import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Text, FloatWritable, AvroKey<java.lang.String>, AvroValue<java.lang.Float>> {
    @Override
    public  void reduce(Text keyIn, Iterable<FloatWritable> valueIn,Context context) throws IOException, InterruptedException {
        float sum=0;
        int count=0;
        for(FloatWritable value:valueIn){
            count++;
            sum+=value.get();
        }
        float average = sum/count;
        context.write(new AvroKey<java.lang.String>(keyIn.toString()), new AvroValue<java.lang.Float>(average));
    }
}
