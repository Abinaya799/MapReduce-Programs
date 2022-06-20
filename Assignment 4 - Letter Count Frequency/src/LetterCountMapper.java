import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LetterCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    @Override
    public void map(LongWritable keyIn,Text valueIn,Context context) throws IOException, InterruptedException {
       String word = valueIn.toString().toLowerCase().replaceAll("[^a-z]","");
       char[] letters = word.toCharArray();
       for (char letter:letters){
           context.write(new Text(String.valueOf(letter)),new IntWritable(1));
       }

    }

}
