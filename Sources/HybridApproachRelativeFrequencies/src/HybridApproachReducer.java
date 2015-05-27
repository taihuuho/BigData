import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import custom.AssociativeArray;
import custom.WordPair;

public class HybridApproachReducer extends
		Reducer<WordPair, IntWritable, Text, AssociativeArray> {

	private int marginal = 0;
	private String currentTerm = null;
	HashMap<String, Float> H = new HashMap<>();

	@Override
	protected void reduce(
			WordPair key,
			Iterable<IntWritable> values,
			Reducer<WordPair, IntWritable, Text, AssociativeArray>.Context context)
			throws IOException, InterruptedException {

		if (currentTerm == null) {
			currentTerm = key.getFirst().toString();
		} else if (!currentTerm.equals(key.getFirst().toString()) && currentTerm != null) {
			for (Entry<String, Float> entry : H.entrySet()) {
				float frequency = (entry.getValue())
						/ (float) marginal;
				entry.setValue(frequency);
			}

			// Emit
			AssociativeArray hashmap =  new AssociativeArray();
			for (Entry<String, Float> entry : H.entrySet()) {
				
				hashmap.put(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
			}
			
			Text text = new Text(currentTerm);
			context.write(text, hashmap);
            
			//Reset
			marginal = 0;
			H = new HashMap<>();
			currentTerm = key.getFirst().toString();
		}
		
		if(key.getSecond() == null){
			H.put(key.getSecond().toString(), 0.0f);
		}
		for (IntWritable intWritable : values) {
			H.put(key.getSecond().toString(), (float)intWritable.get());
			marginal += intWritable.get();
		}
	}

	@Override
	protected void cleanup(
			Reducer<WordPair, IntWritable, Text, AssociativeArray>.Context context)
			throws IOException, InterruptedException {
		
		// emit last pairs
		Text text = new Text(currentTerm);
		AssociativeArray hashmap =  new AssociativeArray();
		for (Entry<String, Float> entry : H.entrySet()) {
			
			hashmap.put(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
		}
		context.write(text, hashmap);	
	}
}
