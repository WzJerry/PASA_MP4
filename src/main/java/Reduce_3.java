import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce_3 extends Reducer<Text, Text,Text, Text> {
    private static int result = 0;
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Result: "), new Text("" + result));
    }
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean flag = false;
        int count = 0;
        for(Text value: values) {
            if(value.toString().equalsIgnoreCase("+")){
                flag = true;
            }else if(value.toString().equalsIgnoreCase("-")) {
                count ++;
            }
        }
        if(flag) {
            result += count;
        }
    }
}
