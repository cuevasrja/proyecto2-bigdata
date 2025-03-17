package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import model.ClassificationModel;
import parser.SpotifyCsvParser;
import spotify.Track;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;

public class RelationPopularity extends Configured implements Tool {
    static SpotifyCsvParser parser = new SpotifyCsvParser();

    public static class SongPopularityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text popularity = new Text();
        private Text trackFeatures = new Text();

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
            if (line.startsWith("\"id\"")) {
                return;
            }

            Track track = parser.parseCSVLine(line);
            Double acousticness = Double.parseDouble(track.get("acousticness"));
            Double danceability = Double.parseDouble(track.get("danceability"));
            Double energy = Double.parseDouble(track.get("energy"));

            if (acousticness >= danceability && acousticness >= energy) {
                trackFeatures.set("acousticness");
            } else if (danceability >= acousticness && danceability >= energy) {
                trackFeatures.set("danceability");
            } else {
                trackFeatures.set("energy");
            }

            popularity.set(track.get("popularity"));
            output.collect(popularity, trackFeatures);
        }
    }

    public static class SongPopularityReducer extends MapReduceBase implements Reducer<Text, Text, Text, FloatWritable> {
        /**
         * Reduce the values for each key.
         * @param key The key.
         * @param values The values.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            int count = 0;
            int acousticness = 0;
            int danceability = 0;
            int energy = 0;

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.equals("acousticness")) {
                    acousticness++;
                } else if (value.equals("danceability")) {
                    danceability++;
                } else {
                    energy++;
                }
                count++;
            }

            float acousticnessPercentage = (float) acousticness / count;
            float danceabilityPercentage = (float) danceability / count;
            float energyPercentage = (float) energy / count;

            output.collect(key, new FloatWritable(acousticnessPercentage));
            output.collect(key, new FloatWritable(danceabilityPercentage));
            output.collect(key, new FloatWritable(energyPercentage));
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), RelationPopularity.class);
        conf.setJobName("relationpopularity");

        conf.setOutputKeyClass(Text.class);
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
