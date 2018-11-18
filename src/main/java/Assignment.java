import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Assignment {
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job1");
        job1.setJarByClass(Assignment.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileSystem fs = FileSystem.get(job1.getConfiguration());

        job1.setMapperClass(Map_1.class);
        job1.setReducerClass(Reduce_1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job1, in);
        FileOutputFormat.setOutputPath(job1, new Path("/user/2018st18/exp4/step1/"));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "job2");
        job2.setJarByClass(Assignment.class);
        job2.setMapperClass(Map_2.class);
        job2.setReducerClass(Reduce_2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2, new Path("/user/2018st18/exp4/step1/"));
        FileOutputFormat.setOutputPath(job2, new Path("/user/2018st/exp4/step2/"));
        job2.waitForCompletion(job1.isComplete());

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "job3");
        job3.setJarByClass(Assignment.class);
        job3.setMapperClass(Map_3.class);
        job3.setReducerClass(Reduce_3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job3, new Path("/user/2018st/exp4/step2/"));
        FileOutputFormat.setOutputPath(job3, out);
        job3.waitForCompletion(job2.isComplete());


    }
}
