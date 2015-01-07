package admt.tmr.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

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
		File output = new File("output");
		if(output.exists())
			delete(output);
		
		TemporalMapReduceMapper.minGranularity = Integer.parseInt(args[2]);
		MAX_TIME = getMaxEndTime(args[1]);
		
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
	
	public static void delete(File file) throws IOException {
 
    	if(file.isDirectory()){
 
    		//directory is empty, then delete it
    		if(file.list().length==0){
 
    		   file.delete();
    		   System.out.println("Directory is deleted : " 
                                                 + file.getAbsolutePath());
 
    		}else{
 
    		   //list all the directory contents
        	   String files[] = file.list();
 
        	   for (String temp : files) {
        	      //construct the file structure
        	      File fileDelete = new File(file, temp);
 
        	      //recursive delete
        	     delete(fileDelete);
        	   }
 
        	   //check the directory again, if empty then delete it
        	   if(file.list().length==0){
           	     file.delete();
        	     System.out.println("Directory is deleted : " 
                                                  + file.getAbsolutePath());
        	   }
    		}
 
    	}else{
    		//if file, then delete it
    		file.delete();
    		System.out.println("File is deleted : " + file.getAbsolutePath());
    	}
    }
	
	public int getMaxEndTime(String file){
		int result = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while(line != null){
				StringTokenizer st = new StringTokenizer(line, ";");
				st.nextToken();
				st.nextToken();
				st.nextToken();
				int tmpMax = Integer.parseInt(st.nextToken());
				if(tmpMax > result)
					result = tmpMax;
				line = br.readLine();
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

}
