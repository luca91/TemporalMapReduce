package admt.tmr.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemporalMapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);
		while(st.hasMoreTokens()){
			StringTokenizer tokens = new StringTokenizer(st.nextToken(), ";");
			tokens.nextToken();
			IntWritable salary = new IntWritable(Integer.parseInt(tokens.nextToken()));
			int sTime = Integer.parseInt(tokens.nextToken());
			int eTime = Integer.parseInt(tokens.nextToken());
			if(TemporalMapReduceDriver.MAX_TIME < eTime)
				TemporalMapReduceDriver.MAX_TIME = eTime;
			for(int i = sTime; i <= eTime; i++){
				Text time = new Text(String.valueOf(i));
				context.write(time, salary);
				for(int j = 1; j <= 4; j++)
					if((i+j) <= TemporalMapReduceDriver.MAX_TIME){
						time = new Text(String.valueOf(i+j));
						context.write(time, salary);
					}
			}

		}
		
	}

}
