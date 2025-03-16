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

public class ClassifyPopularity extends Configured implements Tool{
    static SpotifyCsvParser parser = new SpotifyCsvParser();

    public static class SongPopularityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable[]> {
        private Text album = new Text();
        private DoubleWritable[] trackFeatures = new DoubleWritable[Track.FEATURES+1];

        /**
         * Map the input values.
         * @param key The key.
         * @param value The value.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable[]> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Track track = parser.parseCSVLine(line);
            Double targetValue = Double.parseDouble(track.get("popularity"));
            Double[] features = track.getFeaturesArray();
            for (int i = 0; i < Track.FEATURES; i++) {
                trackFeatures[i] = new DoubleWritable(features[i]);
            }
            trackFeatures[Track.FEATURES] = new DoubleWritable(targetValue);
            album.set(track.get("album_name"));
            output.collect(album, trackFeatures);
        }
    }

    public static class SongPopularityReducer extends MapReduceBase implements Reducer<Text, DoubleWritable[], Text, DoubleWritable[]> {
        /**
         * Convert the array of DoubleWritable to a Weka dataset.
         * @param data The data.
         * @param target The target attribute.
         * @return The dataset.
         */
        public Instances arrayToDataset(Iterator<DoubleWritable[]> data, String target) {
            // Define the attributes
            ArrayList<Attribute> attributes = new ArrayList<>();
            attributes.add(new Attribute("acousticness"));
            attributes.add(new Attribute("danceability"));
            attributes.add(new Attribute("energy"));
            attributes.add(new Attribute("instrumentalness"));
            attributes.add(new Attribute("liveness"));
            attributes.add(new Attribute("loudness"));
            attributes.add(new Attribute("speechiness"));
            attributes.add(new Attribute("tempo"));
            attributes.add(new Attribute("valence"));
            attributes.add(new Attribute("duration"));
            attributes.add(new Attribute("explicit"));
            attributes.add(new Attribute("key"));
            attributes.add(new Attribute("mode"));
            attributes.add(new Attribute("time_signature"));
            attributes.add(new Attribute("followers"));
            attributes.add(new Attribute("album_popularity"));
            attributes.add(new Attribute("artist_popularity"));
            
            // Define the class attribute values
            ArrayList<String> classValues = new ArrayList<>();
            for (int i = 0; i <= 100; i++) {
                classValues.add(String.valueOf(i));
            }
            // Add the class attribute target
            attributes.add(new Attribute(target, classValues));

            // Collect the data values
            List<double[]> dataValues = new ArrayList<>();
            // Add the tracks to the list
            while (data.hasNext()) {
                DoubleWritable[] writableArray = data.next();
                double[] trackFeatures = new double[writableArray.length];
                for (int i = 0; i < writableArray.length; i++) {
                    trackFeatures[i] = writableArray[i].get();
                }
                dataValues.add(trackFeatures);
            }

            // Create the dataset from the data values
            Instances dataset = new Instances("tracks", attributes, dataValues.size());
            dataset.setClassIndex(attributes.size() - 1);

            // Add the tracks to the dataset as instances
            for (double[] values : dataValues) {
                dataset.add(new DenseInstance(1.0, values));
            }

            return dataset;
        }

        /**
         * Reduce the values for each key.
         * @param key The key.
         * @param values The values.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void reduce(Text key, Iterator<DoubleWritable[]> values, OutputCollector<Text, DoubleWritable[]> output, Reporter reporter) throws IOException {
            Instances dataset = arrayToDataset(values, "popularity");
            double albumPopularity = values.hasNext() ? values.next()[15].get() : 0;
            ClassificationModel model = new ClassificationModel("popularity");
            try {
                model.train(dataset);
                double[] predictions = model.predict(dataset);
                model.printMetrics();
                double avg = 0;
                for (double prediction : predictions) {
                    avg += prediction;
                }
                avg /= predictions.length;
                output.collect(key, new DoubleWritable[]{new DoubleWritable(avg), new DoubleWritable(albumPopularity)});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), ClassifyPopularity.class);
        conf.setJobName("classify-popularity");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable[].class);

        conf.setMapperClass(SongPopularityMapper.class);
        conf.setReducerClass(SongPopularityReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        return 0;
    }

    public void execute(String[] args) throws Exception {
        int res = ToolRunner.run(new ClassifyPopularity(), args);
        System.exit(res);
    }
}
