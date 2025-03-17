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

public class GenresPopularities extends Configured implements Tool {
    static SpotifyCsvParser parser = new SpotifyCsvParser();

    public static class GenresPopularitiesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text genre = new Text();
        private Text popularityValues = new Text();

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
            genre.set(track.get("genre_id").equals("") || track.get("genre_id") == null ? "unknown" : track.get("genre_id"));
            int songPopularity = track.get("popularity").equals("") || track.get("popularity") == null ? 0 : Integer.parseInt(track.get("popularity"));
            int albumPopularity = track.get("album_popularity").equals("") || track.get("album_popularity") == null ? 0 : Integer.parseInt(track.get("album_popularity"));
            popularityValues.set(songPopularity + "," + albumPopularity);
            output.collect(genre, popularityValues);
        }
    }

    public static class GenresPopularitiesReducer extends MapReduceBase implements Reducer<Text, Text, Text, FloatWritable> {
        /**
         * Reduce the values for each key.
         * @param key The key.
         * @param values The values.
         * @param output Use to collect the output.
         * @param reporter Facility to report progress.
         * @throws IOException
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            // Accumulate the songs and albums popularity for each genre
            float sumSongPopularity = 0;
            float sumAlbumPopularity = 0;
            // Count the number of songs for each genre
            int count = 0;

            // Iterate over the values associated with the key (genre)
            while (values.hasNext()) {
                String[] actual = values.next().toString().split(",");
                // Accumulate the popularity values of the songs and albums for each genre
                sumSongPopularity += Integer.parseInt(actual[0]);
                sumAlbumPopularity += Integer.parseInt(actual[1]);
                // Add one to the count of songs for each genre
                count++;
            }

            // Calculate the average popularity of the songs 
            float averageSongPopularity = sumSongPopularity / count;
            // Calculate the average popularity of the albums
            float averageAlbumPopularity = sumAlbumPopularity / count;
            // Calculate the difference between the average popularity of the songs and the average popularity of the albums
            float diff = averageSongPopularity - averageAlbumPopularity;
            output.collect(key, new FloatWritable(diff));
        }

        /**
         * El objetivo de este cálculo es determinar cómo la popularidad promedio de las canciones de un género se compara 
         * con la popularidad promedio de los álbumes de ese género. 
         * La diferencia puede interpretarse asi:
         * 
         * Positiva: Significa que la popularidad promedio de las canciones es mayor que la popularidad promedio de los álbumes. 
         * Esto podría indicar que las canciones individuales son más populares que los álbumes en su conjunto.
         * 
         * Negativa: Significa que la popularidad promedio de las canciones es menor que la popularidad promedio de los álbumes. 
         * Esto podría indicar que los álbumes en su conjunto son más populares que las canciones individuales.
         */
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

        JobConf conf = new JobConf(getConf(), GenresPopularities.class);
        conf.setJobName("Get the relationship between genre popularity and their songs popularity");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(GenresPopularitiesMapper.class);
        conf.setReducerClass(GenresPopularitiesReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        return 0;
    }

    public void execute(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GenresPopularities(), args);
        System.exit(res);
    }
}