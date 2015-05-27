import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import custom.WordPair;


public class PairApproachPartitioner extends Partitioner<WordPair, IntWritable>  {

	@Override
	public int getPartition(WordPair pair, IntWritable count, int numReduceTasks) {
		//this is done to avoid performing mod with 0
        if(numReduceTasks == 0)
            return 0;
        
        return pair.baseHashCode() % numReduceTasks;
	}

}
