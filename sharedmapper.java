import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sharedmapper extends
Mapper< Object , Text, IntWritable, IntWritable > {
        IntWritable trackId = new IntWritable();
        IntWritable share = new IntWritable();
 
public void map(Object key, Text value,
    Mapper< Object , Text, IntWritable, IntWritable > .Context context)
        throws IOException, InterruptedException {
 
    String[] parts = value.toString().split("[|]");
    trackId.set(Integer.parseInt(parts[1]));
    share.set(Integer.parseInt(parts[2]));
        
if (parts[1].length ==2) {
        context.write(trackId, share);
}
    }
}