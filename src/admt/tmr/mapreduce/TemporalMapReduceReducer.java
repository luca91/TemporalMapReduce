package admt.tmr.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemporalMapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		if(Integer.parseInt(key.toString()) <= TemporalMapReduceDriver.maxTime){
			int sum = 0, counter = 0;
			
			for(IntWritable value : values){
				sum += value.get();
				counter++;
			}
			
			int average = sum/counter;
			context.write(key, new IntWritable(average));
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Reduce task completed in " + (endTime - startTime) + "ms");
		TemporalMapReduceDriver.reduceTime += (endTime - startTime);
		TemporalMapReduceDriver.reduceTasks++;
	}

}
