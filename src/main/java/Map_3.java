import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map_3 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] line = values.toString().split(" ");
        context.write(new Text(line[0]), new Text(line[1]));
    }
}
