import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostPopularDomains {

    // Mapper class
    // LongWritable for the byte offset of the current line, Text for the line
    // itself
    // Text for the domain name output, and IntWritable for count = 1
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Extracting the client's machine name from the line
            int firstSpace = line.indexOf(" ");
            String fullDomain = line.substring(0, firstSpace);

            // If the domain is only IP addresses, skip it
            if (!Character.isLetter(fullDomain.charAt(0))) {
                return;
            }

            // Extracting the domain name from client's machine name
            int lastDot = fullDomain.lastIndexOf(".");
            int dotBeforeLastDot = fullDomain.lastIndexOf(".", lastDot - 1);
            String domain = fullDomain.substring(dotBeforeLastDot + 1);

            context.write(new Text(domain), one);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<String, Integer> countMap = new TreeMap<String, Integer>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Sum the frequency of each domain
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Store the domain if it doesn't exist in the map
            if (!countMap.containsKey(key.toString())) {
                countMap.put(key.toString(), sum);
            }
            // Update the count if the domain already exists in the map
            else {
                countMap.put(key.toString(), countMap.get(key.toString()) + sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the map by value
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(countMap.entrySet());
            list.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            // Output the top 5 most popular domain names
            for (int i = 0; i < 5; i++) {
                Map.Entry<String, Integer> entry = list.get(i);
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MostPopularDomains.class);
        job.setMapperClass(TokenizerMapper.class);

        // Combiner class
        job.setCombinerClass(IntSumReducer.class);

        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the number of reducers to 1 for cumulative output
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
