import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<Text, FloatWritable> {

    @Override
    public int getPartition(Text text, FloatWritable floatWritable, int numPartitions) {
        if(numPartitions == 0)
        {
            return 0;
        }

        String reduceCriterion = text.toString();
        if(reduceCriterion.matches("[0-2]{1}\\d*")){
            return 0;
        }
        else if (reduceCriterion.matches("[3-5]{1}\\d*")){
            return 1%numPartitions;
        }
        else if (reduceCriterion.matches("[6-8]{1}\\d*")){
            return 2%numPartitions;
        }
        else if (reduceCriterion.matches("[9]{1}\\d*")){
            return 3%numPartitions;
        }
        else{
            return 4%numPartitions;
        }
    }
}
