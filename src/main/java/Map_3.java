import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map_3 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        StringTokenizer st=new StringTokenizer(values.toString());
        context.write(new Text(st.nextToken()), new Text(st.nextToken()));
    }
}
