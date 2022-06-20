import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LetterFrequencyMapper extends Mapper<AvroKey<LetterCountOutputAvro>, NullWritable,AvroKey<String>, AvroValue<LetterCountOutputAvro>> {

    public void map(AvroKey<LetterCountOutputAvro> keyIn,NullWritable valueIn,Context context) throws IOException, InterruptedException {
        String keyOut = "Record";
        String letter = (String) keyIn.datum().getLetter();
        int count = keyIn.datum().getCount();
        LetterCountOutputAvro record = new LetterCountOutputAvro();
        record.setLetter(letter);
        record.setCount(count);
        context.write(new AvroKey<>(keyOut),new AvroValue<>(record));
    }
}
