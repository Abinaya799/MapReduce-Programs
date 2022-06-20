import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterCountReducer extends Reducer<Text, IntWritable, AvroKey<LetterCountOutputAvro>, NullWritable> {
    public void reduce(Text letter,Iterable<IntWritable> valueIn, Reducer<Text,IntWritable,AvroKey<LetterCountOutputAvro>, NullWritable>.Context context) throws IOException, InterruptedException {
        int sum =0;
        while (valueIn.iterator().hasNext()){
            sum +=valueIn.iterator().next().get();
        }
        LetterCountOutputAvro rec = new LetterCountOutputAvro();
        rec.setLetter(letter.toString());
        rec.setCount(sum);
        context.write(new AvroKey<>(rec),NullWritable.get());
    }
}
