import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map_2 extends Mapper<LongWritable ,Text ,Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\+");
        context.write(new Text(line[0]), new Text(line[1]));
    }
}
