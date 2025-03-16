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

    public static class ArtistsPopularitiesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable[]> {
        private Text artist = new Text();
        private IntWritable songPopularity = new IntWritable();
        private IntWritable artistPopularity = new IntWritable();

        /**
         * Map the input values.
         * @param key The key.
         * @param value The value.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable[]> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Track track = parser.parseCSVLine(line);
            artist.set(track.get("artist_name"));
            songPopularity.set(Integer.parseInt(track.get("popularity")));
            artistPopularity.set(Integer.parseInt(track.get("artist_popularity")));
            output.collect(artist, new IntWritable[]{songPopularity, artistPopularity});
        }
    }
    
    public static class ArtistsPopularitiesReducer extends MapReduceBase implements Reducer<Text, IntWritable[], Text, FloatWritable> {
        /**
         * Reduce the values for each key.
         * @param key The key.
         * @param values The values.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void reduce(Text key, Iterator<IntWritable[]> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            IntWritable[] actual = values.next();
            int artistPopularity = actual[1].get();
            int count = 1;
            do {
                sum += actual[0].get();
                actual = values.next();
                count++;
            } while (values.hasNext());
            sum /= count;
            float diff = sum - artistPopularity;
            output.collect(key, new FloatWritable(diff));
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
        conf.setOutputValueClass(FloatWritable.class);

        conf.setMapperClass(ArtistsPopularitiesMapper.class);
        conf.setReducerClass(ArtistsPopularitiesReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        return 0;
    }
    
    public void execute(String[] args) throws Exception {
        int res = ToolRunner.run(new ArtistsPopularities(), args);
        System.exit(res);
    }
}
