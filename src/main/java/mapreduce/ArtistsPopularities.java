package mapreduce;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import parser.SpotifyCsvParser;
import spotify.Track;

public class ArtistsPopularities extends Configured implements Tool {
    static SpotifyCsvParser parser = new SpotifyCsvParser();


    public static class ArtistsPopularitiesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        // private final static IntWritable one = new IntWritable(1);
        // private Text artist = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Track track = parser.parseCSVLine(line);
            // if (parts.length == 19) {
            //     artist.set(parts[2]);
            //     output.collect(artist, one);
            // }
        }

    }
    
    public static class ArtistsPopularitiesReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            // int sum = 0;
            // while (values.hasNext()) {
            //     sum += values.next().get();
            // }
            // output.collect(key, new IntWritable(sum));
        }

    }


    /**
     * Run the tool.
     * @param args command specific arguments.
     * - CSV input file
     * - Output directory
     * @return exit code.
     */
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];

        JobConf conf = new JobConf(getConf(), ArtistsPopularities.class);
        conf.setJobName("Get the relationship between artists popularity and their songs popularity");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(ArtistsPopularitiesMapper.class);
        conf.setReducerClass(ArtistsPopularitiesReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        return 0;
    }
    
}
