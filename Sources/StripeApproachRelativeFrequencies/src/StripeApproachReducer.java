import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import custom.AssociativeArray;


public class StripeApproachReducer extends
		Reducer<Text, AssociativeArray, Text, AssociativeArray> {
	@Override
	protected void reduce(Text term, Iterable<AssociativeArray> stripes,
			Reducer<Text, AssociativeArray, Text, AssociativeArray>.Context context)
			throws IOException, InterruptedException {
		AssociativeArray H = new AssociativeArray();
		
		int count = 0;
		for (AssociativeArray stripe : stripes) {
			for (Writable key : stripe.keySet()) {
				int value = ((IntWritable)stripe.get(key)).get();
				
				count += value;
				if (H.containsKey(key)){
					IntWritable intWrit = (IntWritable) H.get(key);
					int newValue = intWrit.get() + value;
					intWrit.set(newValue);
				}else{
					H.put(key, new IntWritable(value));
				}
			}
		}
		
		// Emits term and stripe
		for(Writable key : H.keySet()){
			int value = ((IntWritable) H.get(key)).get();
			float freq = (float)value/count;
			H.put(key, new FloatWritable(freq));
		}
		context.write(term, H);
	}
}
