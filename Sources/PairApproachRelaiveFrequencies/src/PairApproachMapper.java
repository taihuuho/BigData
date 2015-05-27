import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import custom.WordPair;

public class PairApproachMapper extends
		Mapper<Object, Text, WordPair, IntWritable> {

	HashMap<String, Integer> hash = new HashMap<String, Integer>();

	@Override
	protected void cleanup(
			Mapper<Object, Text, WordPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// emit all pairs
		for (Entry<String, Integer> entry : hash.entrySet()) {
			String[] tokens = entry.getKey().split(WordPair.DELIM);
			WordPair wordPair = new WordPair(tokens[0], tokens[1]);
			context.write(wordPair, new IntWritable(entry.getValue()));
		}
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split("\\s+");
		System.out.println(words);
		for (int i = 0; i < words.length - 1; i++) { // we don't need to map the
														// last word
			words[i] = words[i].replaceAll("\\W+", "");
			if (words[i].equals("")) {
				continue;
			}
			for (int j = i + 1; j < words.length; j++) {// process all neighbors
				if (words[i].equals(words[j])
						|| words[j].replaceAll("\\W+", "").equals(""))
					break;
				else {
					WordPair wordPair = new WordPair(words[i], words[j]);
					if (hash.containsKey(wordPair.keyString())){
						hash.put(wordPair.keyString(), hash.get(wordPair.keyString()) + 1);
					}else{
						hash.put(wordPair.keyString(), 1);
					}
			
					WordPair wordPair2 = new WordPair(words[i],
							WordPair.ASTERISK);
					if (hash.containsKey(wordPair2.keyString())){
						hash.put(wordPair2.keyString(), hash.get(wordPair2.keyString()) + 1);
					}else{
						hash.put(wordPair2.keyString(), 1);
					}
				}
			}
		}
	}
}
