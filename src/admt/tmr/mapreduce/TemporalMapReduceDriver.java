package admt.tmr.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
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
	
	public static int maxTime = 0;
	public static int mappingTime = 0;
	public static int reduceTime = 0;
	public static int mappingTasks = 0;
	public static int reduceTasks = 0;
	public static int keyValuePairs = 0;
	public static int timeUnits = 0;
	public static int rows = 0;

	@Override
	public int run(String[] args) throws Exception {
		File output = new File("./output");
		if(output.exists())
			delete(output);
		
		TemporalMapReduceMapper.minGranularity = Integer.parseInt(args[2]);
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "tmr");
		job.setJarByClass(TemporalMapReduceDriver.class);
		
		job.setMapperClass(TemporalMapReduceMapper.class);
		job.setReducerClass(TemporalMapReduceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		maxTime = getMaxEndTime(args[0]);
		System.out.println("Max time: " + maxTime);
		int res = ToolRunner.run(new Configuration(),  new TemporalMapReduceDriver(), args);
		System.out.println(res)	;
		coalesce(extractResults());
		printStatistics();
	}
	
	public static void delete(File file) throws IOException {
 
    	if(file.isDirectory()){
 
    		//directory is empty, then delete it
    		if(file.list().length==0){
 
    		   file.delete();
    		   System.out.println("Directory is deleted : " + file.getAbsolutePath());
 
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
        	     System.out.println("Directory is deleted : "  + file.getAbsolutePath());
        	   }
    		}
 
    	}else{
    		//if file, then delete it
    		file.delete();
    		System.out.println("File is deleted : " + file.getAbsolutePath());
    	}
    }
	
	public static int getMaxEndTime(String file){
		int result = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while(line != null){
				rows++;
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
	
	public static HashMap<Integer, Integer> extractResults(){
		HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("output/part-r-00000"));
			String line = br.readLine();
			while(line != null){
				StringTokenizer st = new StringTokenizer(line, "\t");
				result.put(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
				try {
					line = br.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static void coalesce(HashMap<Integer, Integer> input){
		File output = new File("output");
		int start = -1, end = -1;
		int value = -1, old = -1;
		if(output.isDirectory()){
			start = -1;
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter("output/part-r-00000-coalesced"));
				int aux = -1;
				for(int i = 0; i <= maxTime; i++){
//					bw.write(i + "\t" + input.get(i) + "\n");
					if(input.get(i) != null){
						if(start == -1){
							start = i;
							aux = start;
							old = input.get(i);
							value = old;
						}
						else{
							aux = i;
							value = input.get(i);
						}
						if(value == old)
							end = aux;
						else{
							bw.write("[" + start + "-" + end + "]\t" + old + "\n");
							bw.flush();
							start = aux;
							old = value;
							end = start;
						}
					}
				}
				end = aux;
				bw.write("[" + start + "-" + end + "]\t" + old + "\n");
				bw.flush();
				bw.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void printStatistics(){
		System.out.println("Mapping tasks: " + mappingTasks);
		System.out.println("Reduce tasks: " + reduceTasks);
		System.out.println("Total mapping time: " + mappingTime + "ms");
		System.out.println("Total reduce time: " + reduceTime + "ms");
		System.out.println("Average mapping time: " + (mappingTime/mappingTasks) + "ms");
		System.out.println("Average reduce time: " + (reduceTime/reduceTasks) + "ms");
		System.out.println("Average time points created per row: " + (timeUnits/rows));
	}

}
