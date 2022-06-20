import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

public class LetterFrequencyReducer extends Reducer<AvroKey<String>, AvroValue<LetterCountOutputAvro>, NullWritable, OrcStruct> {
    private final TypeDescription orcSchema = TypeDescription.fromString("struct<letter:string,frequency:double>");
    private final OrcStruct outputOrc = (OrcStruct) OrcStruct.createValue(orcSchema);

    @Override
    public void reduce(AvroKey<String> keyIn, Iterable<AvroValue<LetterCountOutputAvro>> valueIn,
                       Reducer<AvroKey<String>, AvroValue<LetterCountOutputAvro>, NullWritable, OrcStruct>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (AvroValue<LetterCountOutputAvro> val : valueIn) {
            count += val.datum().getCount();
        }
        for (AvroValue<LetterCountOutputAvro> val : valueIn) {
            String letter = val.datum().getLetter().toString();
            double frequency = (double) (val.datum().getCount()) / count;
            outputOrc.setFieldValue(0, new Text(letter));
            outputOrc.setFieldValue(1, new DoubleWritable(frequency));
            context.write(NullWritable.get(), outputOrc);
        }
    }
}
