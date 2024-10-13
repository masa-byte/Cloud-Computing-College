import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper class
    // Object for input key, Text for input value (file line)
    // Text for output key (word), IntWritable for output value (count = 1)
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                // Output word with count 1
                context.write(word, one);
            }
        }
    }

    // Reducer class
    // Text for input key (word), IntWritable for input value (count = 1)
    // Text for output key (word), IntWritable for output value (count)
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // TreeMap to store the words and their counts
        private TreeMap<String, Integer> countMap = new TreeMap<String, Integer>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // If key begins with a non-letter character, skip it
            if (!Character.isLetter(key.toString().charAt(0))) {
                return;
            }

            // Calculate the sum of the counts
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Store the word if it doen't exist in the map
            if (!countMap.containsKey(key.toString())) {
                countMap.put(key.toString(), sum);
            }
            // Update the count if the word already exists in the map
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

            // Output the top 100 most frequent words
            for (int i = 0; i < 100; i++) {
                Map.Entry<String, Integer> entry = list.get(i);
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
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
