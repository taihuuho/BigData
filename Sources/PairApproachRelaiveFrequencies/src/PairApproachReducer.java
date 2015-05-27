import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import custom.WordPair;


public class PairApproachReducer extends Reducer<WordPair, IntWritable, WordPair, FloatWritable> {

	private int asteriskCount = 0;

	@Override
	protected void reduce(WordPair key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        String second = key.getSecond().toString();
        System.out.println("second: "+ second);
        if (second.equalsIgnoreCase(WordPair.ASTERISK)) {
			asteriskCount = count;
		}else{
			context.write(key, new FloatWritable((float)count/asteriskCount));
		}
        
	}
}
