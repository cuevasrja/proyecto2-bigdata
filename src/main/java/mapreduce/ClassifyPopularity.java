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

    public static class SongPopularityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text album = new Text();
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
            Double targetValue = Double.parseDouble(track.get("popularity").equals("") || track.get("popularity") == null ? "0" : track.get("popularity"));
            String[] features = track.getFeaturesArrayToString();
            String featuresString = "";
            for (int i = 0; i < features.length; i++) {
                featuresString += features[i] + ",";
            }
            featuresString += targetValue.toString();
            album.set(track.get("album_name").equals("") || track.get("album_name") == null ? "unknown" : track.get("album_name"));
            trackFeatures.set(featuresString);
            output.collect(album, trackFeatures);
        }
    }

    public static class SongPopularityReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        ClassificationModel model = new ClassificationModel("popularity");
        /**
         * Convert the array of DoubleWritable to a Weka dataset.
         * @param data The data.
         * @param target The target attribute.
         * @return The dataset.
         */
        public Instances arrayToDataset(Iterator<Text> data, String target, Text acc) {
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
                Text node = data.next();
                String[] writableArray = node.toString().split(",");
                double[] trackFeatures = new double[writableArray.length];
                for (int i = 0; i < writableArray.length; i++) {
                    trackFeatures[i] = Double.parseDouble(writableArray[i].equals("") || writableArray[i] == null ? "0" : writableArray[i]);
                }
                acc.set(node.toString());
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
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Text node = new Text();

            // Create the dataset
            Instances dataset = arrayToDataset(values, "popularity", node);

            // Get the album popularity
            String[] valuesArray = node.toString().split(",");
            double albumPopularity = valuesArray.length >= 16 ? Double.parseDouble(valuesArray[15]) : 0;

            // Train the model
            try {
                model.train(dataset);
                // Predict the popularity
                double[] predictions = model.predict(dataset);

                // Calculate the average prediction
                double avg = 0;
                for (double prediction : predictions) {
                    avg += prediction;
                }
                avg /= predictions.length;

                // Output the album and the popularity prediction
                output.collect(key, new Text("Popularity Prediction: " + avg + " Album Popularity: " + albumPopularity));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), ClassifyPopularity.class);
        conf.setJobName("classify-popularity");

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
        int res = ToolRunner.run(new ClassifyPopularity(), args);
        System.exit(res);
    }
}
