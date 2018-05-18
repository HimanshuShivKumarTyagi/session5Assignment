import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class songhearedreducer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(
				IntWritable trackId,
				Iterable<IntWritable> heard,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int counter =0;
			for (IntWritable heard : heard) {
				if(heard==1)
                      {
                        counter++
                      }
			}
		
			context.write(trackId, counter);
		}
	}