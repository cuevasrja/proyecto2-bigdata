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

    public static class ArtistsPopularitiesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text artist = new Text();
        private Text songPopularity = new Text();

        /**
         * Map the input values.
         * @param key The key.
         * @param value The value.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();

            // Skip the header
            if (line.startsWith("\"id\"")) {
                return;
            }

            // Parse the CSV line
            Track track = parser.parseCSVLine(line);

            // Get the artist name and song popularity
            artist.set(track.get("artist_name"));
            String songPopularityString = track.get("popularity");
            String artistPopularity = track.get("artist_popularity").equals("") ? "0" : track.get("artist_popularity");
            songPopularity.set(songPopularityString + "," + artistPopularity);
            output.collect(artist, songPopularity);
        }
    }

    public static class ArtistsPopularitiesReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        /**
         * Reduce the values for each key.
         * @param key The key.
         * @param values The values.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // Initialize the sum and count
            float sum = 0;
            int count = 0;
            float artistPopularity = 0;

            // Calculate the sum and count
            while (values.hasNext()) {
                String[] songPopularity = values.next().toString().split(",");
                // Get the song popularity and sum it
                sum += Float.parseFloat(songPopularity[0]);
                // Get the artist popularity
                artistPopularity = Float.parseFloat(songPopularity[1]);
                // Increment the count
                count++;
            }

            // Calculate the average
            float avg = sum / count;

            // Output the artist and the average popularity
            output.collect(key, new Text("Average song popularity: " + avg + ", Artist popularity: " + artistPopularity));
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
        conf.setOutputValueClass(Text.class);

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