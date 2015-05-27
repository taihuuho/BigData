import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import custom.AssociativeArray;


public class StripeApproachPartitioner extends Partitioner<Text, AssociativeArray> {

	@Override
	public int getPartition(Text term, AssociativeArray stripe, int numReduceTask) {
		if (numReduceTask == 0){
			return 0;
		}
		
		return term.toString().hashCode() % numReduceTask;
	}

}
