import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		
        if(numReduceTasks == 0)
            return 0;

        /* let the partitioner  assign all words less than letter 'k' to Reducer 1,  
        all words greater than 'r' to Reducer 3 and  everything else to Reducer 2.
        */
        String keyValue = key.toString();
        if(keyValue.compareTo("k") < 0){               
            return 0;
        }
        else if(keyValue.compareTo("r") > 0){
           return 2;
        }
        //otherwise assign partition 2
        else
        	return 1;
	}

}
