import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueListenersMapper extends
Mapper< Object , Text, IntWritable, IntWritable > {
        IntWritable trackId = new IntWritable();
        IntWritable userId = new IntWritable();
 
public void map(Object key, Text value,
    Mapper< Object , Text, IntWritable, IntWritable > .Context context)
        throws IOException, InterruptedException {
 
    String[] parts = value.toString().split("[|]");
    trackId.set(Integer.parseInt(parts[1]));
    userId.set(Integer.parseInt(parts[0]));
        
if (parts.length ==5) {
        context.write(trackId, userId);
    }
else{     
context.getCounter(UniqueListenersDriver.COUNTERS.INVALID_RECORD_COUNT).increment(1L);
    }
    }
}