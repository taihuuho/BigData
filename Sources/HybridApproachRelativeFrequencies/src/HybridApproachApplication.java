import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import custom.AssociativeArray;
import custom.WordPair;


public class HybridApproachApplication extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new HybridApproachApplication(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2){
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}
		
		// Create configuration
				Configuration conf = new Configuration(true);
				Path inputPath = new Path(args[0]);
				Path outputPath = new Path(args[1]);

				Job job = Job.getInstance(conf);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(AssociativeArray.class);

				job.setMapOutputKeyClass(WordPair.class);
				job.setMapOutputValueClass(IntWritable.class);

				job.setMapperClass(HybridApproachInMapperCombiningMapper.class);
				job.setReducerClass(HybridApproachReducer.class);
				job.setPartitionerClass(HybridApproachPartitioner.class);
				job.setNumReduceTasks(1);

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				FileInputFormat.setInputPaths(job, inputPath);
				FileOutputFormat.setOutputPath(job, outputPath);

				job.setJarByClass(HybridApproachApplication.class);

				// Delete output if exists
				FileSystem hdfs = FileSystem.get(conf);
				if (hdfs.exists(outputPath))
					hdfs.delete(outputPath, true);

				// Execute job
				int code = job.waitForCompletion(true) ? 0 : 1;

				return code;
	}

}
