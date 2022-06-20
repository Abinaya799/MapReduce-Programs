import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class LetterCountPartitioner extends Partitioner<org.apache.hadoop.io.Text, IntWritable> {
    @Override
    public int getPartition(org.apache.hadoop.io.Text text, IntWritable intWritable, int i) {
        if (text.toString().equals("[a-m]")) {
            return 0;
        } else {
            return 1;
        }
    }
}
