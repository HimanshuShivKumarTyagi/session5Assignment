import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class songhearedmapper extends
Mapper< Object , Text, IntWritable, IntWritable > {
        IntWritable trackId = new IntWritable();
        IntWritable heardfully = new IntWritable();
 
public void map(Object key, Text value,
    Mapper< Object , Text, IntWritable, IntWritable > .Context context)
        throws IOException, InterruptedException {
 
    String[] parts = value.toString().split("[|]");
    trackId.set(Integer.parseInt(parts[1]));
    heardfully.set(Integer.parseInt(parts[4]));
        
if (parts[1].length ==2) {
        context.write(trackId, heardfully);
}
    }
}