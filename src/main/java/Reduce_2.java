import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class Reduce_2 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> array = new ArrayList<String>();
        for(Text value: values){
            array.add(value.toString());
            context.write(new Text(key.toString() + " " + value.toString()), new Text("+"));
        }
        for(int i = 0; i < array.size(); i++) {
            for(int j = i+1; j < array.size(); j++){
                String a = array.get(i);
                String b = array.get(j);
                if(a.compareTo(b) < 0){
                    context.write(new Text(a + "+" + b), new Text("-"));
                }else {
                    context.write(new Text(b + "+" + a), new Text("-"));
                }
            }
        }
    }
}
