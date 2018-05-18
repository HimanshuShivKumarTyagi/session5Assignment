

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UniqueListenersDriver {
	public static enum COUNTERS {
		INVALID_RECORD_COUNT
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
	
    	    if (args.length != 2) {
    	        System.err.println("UniqueListenersDriver <input path> <output path>");
    	        System.exit(-1);
    	      }
  
            
    		//Job Related Configurations
    		Configuration conf = new Configuration();
    		Job job = new Job(conf, "Unique Listener Driver");
    		job.setJarByClass(UniqueListenersDriver.class);

         
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            
            job.setMapperClass(UniqueListenersMapper.class);
            job.setReducerClass(UniqueListenersReducer.class);
            
            Path inp = new Path(args[0]);
            Path out = new Path(args[1]);
            
          //Provide paths to pick the input file for the job
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            
            
            //Provide paths to pick the output file for the job, and delete it if already present
        	Path outputPath = new Path(args[1]);
        	FileOutputFormat.setOutputPath(job, outputPath);
        	outputPath.getFileSystem(conf).delete(outputPath, true);
            //FileOutputFormat.setOutputPath(job, new Path(args[1]));
           
        	//execute the job
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            
          //Some std outs for the counters to come on the console
            Counters counters = job.getCounters();
            
            Counter invalidRecordCount = counters.findCounter(UniqueListenersDriver.COUNTERS.INVALID_RECORD_COUNT);
            System.out.println(invalidRecordCount.getDisplayName()+ " : " + invalidRecordCount.getValue());
            
      }


}

	
	