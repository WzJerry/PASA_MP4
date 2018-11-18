import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map_1 extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");
        String a = new String(line[0]);
        String b = new String(line[1]);
        if(a.compareTo(b) > 0) {
            context.write(new Text(b + "+" + a), new Text("+"));
        }else if(a.compareTo(b) < 0) {
            context.write(new Text(a + "+" + b), new Text("+"));
        }
    }
}
