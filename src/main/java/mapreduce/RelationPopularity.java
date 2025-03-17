package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import parser.SpotifyCsvParser;
import spotify.Track;

public class RelationPopularity extends Configured implements Tool {
    static SpotifyCsvParser parser = new SpotifyCsvParser();

    public static class SongPopularityMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable popularity = new IntWritable();
        private Text trackFeatures = new Text();

        /**
         * Map the input values.
         * @param key The key.
         * @param value The value.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();

            // Skip the header
            if (line.startsWith("\"id\"")) {
                return;
            }

            // Parse the CSV line
            Track track = parser.parseCSVLine(line);

            // Get the acousticness, danceability and energy values
            Double acousticness = Double.parseDouble(track.get("acousticness"));
            Double danceability = Double.parseDouble(track.get("danceability"));
            Double energy = Double.parseDouble(track.get("energy"));

            // Find the highest value between acousticness, danceability and energy
            if (acousticness >= danceability && acousticness >= energy) {
                trackFeatures.set("acousticness");
            } else if (danceability >= acousticness && danceability >= energy) {
                trackFeatures.set("danceability");
            } else {
                trackFeatures.set("energy");
            }

            // Set the popularity as the key
            popularity.set(track.get("popularity").equals("") ? 0 : Integer.parseInt(track.get("popularity")));
            output.collect(popularity, trackFeatures);
        }
    }

    public static class SongPopularityReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        /**
         * Reduce the values for each key.
         * @param key The key.
         * @param values The values.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            // Count the number of occurrences of each feature
            int count = 0;
            int acousticness = 0;
            int danceability = 0;
            int energy = 0;

            // While there are values to process
            while (values.hasNext()) {
                String value = values.next().toString();
                // If the value is acousticness, increment the acousticness counter
                if (value.equals("acousticness")) {
                    acousticness++;
                } else if (value.equals("danceability")) {
                    danceability++;
                } else {
                    energy++;
                }
                // Increment the count
                count++;
            }

            // Calculate the percentage of each feature
            float acousticnessPercentage = (float) acousticness / count;
            float danceabilityPercentage = (float) danceability / count;
            float energyPercentage = (float) energy / count;

            // Output the key and the percentages of each feature
            output.collect(key, new Text("acousticness: " + acousticnessPercentage + ", danceability: " + danceabilityPercentage + ", energy: " + energyPercentage));
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), RelationPopularity.class);
        conf.setJobName("relationpopularity");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(SongPopularityMapper.class);
        conf.setReducerClass(SongPopularityReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public void execute(String[] args) throws Exception {
        int res = ToolRunner.run(new RelationPopularity(), args);
        System.exit(res);
    }
}
