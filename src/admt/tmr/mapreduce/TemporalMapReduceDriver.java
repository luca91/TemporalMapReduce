package admt.tmr.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TemporalMapReduceDriver extends Configured implements Tool {
	
	public static int MAX_TIME = 0;

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Temporal Map Reduce");
		job.setJarByClass(TemporalMapReduceDriver.class);
		
		job.setMapperClass(TemporalMapReduceMapper.class);
		job.setReducerClass(TemporalMapReduceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.	setOutputPath(job, new Path(args[1]));
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),  new TemporalMapReduceDriver(), args);
		System.out.println(res)	;
	}
	
//	public static void setMaxTime(int maxTime){
//		MAX_TIME = maxTime;
//	}

}
