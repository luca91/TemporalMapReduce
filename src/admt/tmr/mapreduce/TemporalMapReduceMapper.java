package admt.tmr.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemporalMapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public static int minGranularity;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);
		while(st.hasMoreTokens()){
			StringTokenizer tokens = new StringTokenizer(st.nextToken(), ";");
			tokens.nextToken();
			IntWritable salary = new IntWritable(Integer.parseInt(tokens.nextToken()));
			int sTime = Integer.parseInt(tokens.nextToken());
			int eTime = Integer.parseInt(tokens.nextToken());
			int i = 0;
			for(i = sTime - (sTime % minGranularity); i <= eTime; i += minGranularity){
				int t = (i/minGranularity);
//				System.out.println("Key: " + t);
				Text time = new Text(String.valueOf(t));
				context.write(time, salary);
				TemporalMapReduceDriver.keyValuePairs++;
				TemporalMapReduceDriver.timeUnits++;
				for(int j = minGranularity; j < minGranularity*4; j += minGranularity){
					int p = (t+(j / minGranularity)); 
					if((i+j) < TemporalMapReduceDriver.maxTime){
//						System.out.println("Key moving inner:" + p);
//						System.out.println("MAX inner: " + TemporalMapReduceDriver.MAX_TIME);
						time = new Text(String.valueOf(p));
						context.write(time, salary);
						TemporalMapReduceDriver.keyValuePairs++;
					}
				}
			}
			if(i > eTime){
				Text time = new Text(String.valueOf(i/minGranularity));
				context.write(time, salary);
				TemporalMapReduceDriver.keyValuePairs++;
				TemporalMapReduceDriver.timeUnits++;
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Mapping task completed in " + (endTime - startTime) + "ms");
		TemporalMapReduceDriver.mappingTime += (endTime - startTime);
		TemporalMapReduceDriver.mappingTasks++;
	}
	
}
