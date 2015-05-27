import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import custom.AssociativeArray;

public class StripeApproachMapper extends
		Mapper<Object, Text, Text, AssociativeArray> {
	private HashMap<String, HashMap<String, Integer>> hash = new HashMap<String, HashMap<String, Integer>>();
	@Override
	protected void cleanup(
			Mapper<Object, Text, Text, AssociativeArray>.Context context)
			throws IOException, InterruptedException {

		// emit all terms with their relevant stripe
		for (String key : hash.keySet()) {
			AssociativeArray stripe = new AssociativeArray();
			HashMap<String, Integer> hashStripe =  hash.get(key);
			for (String neighbor : hashStripe.keySet()) {
				stripe.put(new Text(neighbor), new IntWritable(hashStripe.get(neighbor)));
			}
			context.write(new Text(key), stripe);
		}
	}

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, AssociativeArray>.Context context)
			throws IOException, InterruptedException {
		// all lines of a doc
		String[] lines = value.toString().split("\\n");
		// for each line
		for (String line : lines) {
			// split the line into a list of strings
			String[] words = line.toString().split("\\s+");

			for (int i = 0; i < words.length - 1; i++) {
				if (words[i].equals("")) {
					continue;
				}
				String term = words[i];
				for (int j = i + 1; j < words.length; j++) {
					if (words[j].equals(term)
							|| words[j].replaceAll("\\W+", "").equals("")) { 
						break;
					}
					if (!hash.containsKey(term)) {
						hash.put(term, new HashMap<String, Integer>());
					}

					String neighbor = words[j];
					HashMap<String, Integer> stripeHash = hash.get(term);
					if (stripeHash.containsKey(neighbor)) {

						Integer count = stripeHash
								.get(neighbor);
						stripeHash.put(neighbor, count + 1);
					} else {
						stripeHash.put(neighbor, 1);
					}
				}
			}
		}
	}
}
