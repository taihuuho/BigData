import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import custom.WordPair;

public class HybridApproachInMapperCombiningMapper extends
		Mapper<Object, Text, WordPair, IntWritable> {

	HashMap<WordPair, Integer> hash = new HashMap<WordPair, Integer>();

	@Override
	protected void cleanup(
			Mapper<Object, Text, WordPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// emit all pairs
		for (Entry<WordPair, Integer> entry : hash.entrySet()) {
			context.write(entry.getKey(), new IntWritable(entry.getValue()));
		}
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		System.out.println(words);
		for (int i = 0; i < words.length - 1; i++) { // we don't need to map the
														// last word
			words[i] = words[i].replaceAll("\\W+", "");
			if (words[i].equals("")) {
				continue;
			}
			for (int j = i + 1; j < words.length; j++) {// process all neighbors
				if (words[i].equals(words[j])
						|| words[j].replaceAll("\\W", "").equals(""))
					break;
				else {
					WordPair wordPair = new WordPair(words[i], words[j]);
					Integer cnt = hash.get(wordPair);

					if (cnt == null) {
						cnt = 0;
					}
					hash.put(wordPair, cnt + 1);
				}
			}
		}
	}
}
