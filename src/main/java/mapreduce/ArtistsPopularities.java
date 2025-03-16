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

    public static class ArtistsPopularitiesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text artist = new Text();
        private FloatWritable songPopularity = new FloatWritable();

        /**
         * Map the input values.
         * @param key The key.
         * @param value The value.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            if (line.startsWith("\"id\"")) {
                return;
            }

            Track track = parser.parseCSVLine(line);
            artist.set(track.get("artist_name"));
            songPopularity.set(Float.parseFloat(track.get("popularity")));
            output.collect(artist, songPopularity);
        }
    }

    public static class ArtistsPopularitiesReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {
        /**
         * Reduce the values for each key.
         * @param key The key.
         * @param values The values.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            int count = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                count++;
            }
            float avg = sum / count;
            output.collect(key, new FloatWritable(avg));
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