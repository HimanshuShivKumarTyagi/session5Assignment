

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
	

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
	
     
            
    		//Job Related Configurations
    		Configuration conf = new Configuration();
    		Job job = new Job(conf, "Unique Listener Driver");
    		job.setJarByClass(UniqueListenersDriver.class);

         
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            
            job.setMapperClass(songhearedmapper.class);
            job.setReducerClass(songhearedreducer.class);
            
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
            
            
      }


}

	
	