package parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import spotify.Track;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;

public class SpotifyCsvParser {
    public static final Iterable<String> CSV_HEADER = Arrays.asList(
        "id",
        "track_name",
        "disc_number",
        "duration",
        "explicit",
        "audio_feature_id",
        "preview_url",
        "track_number",
        "popularity",
        "is_playable",
        "acousticness",
        "danceability",
        "energy",
        "instrumentalness",
        "key",
        "liveness",
        "loudness",
        "mode",
        "speechiness",
        "tempo",
        "time_signature",
        "valence",
        "album_name",
        "album_group",
        "album_type",
        "release_date",
        "album_popularity",
        "artist_name",
        "artist_popularity",
        "followers",
        "genre_id"
    );

    /**
     * Parse the records in a text file.
     * @param value
     * @return A list of records.
     * @throws IOException
     */
    public List<Track> parse(Text value) throws IOException {
        System.out.println("\033[1;94mParsing file...\033[0m");
        // Create a list to store the records
        List<Track> records = new ArrayList<>();

        // Create a configuration object and a file system object
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Get the path to the file
        Path path = new Path(value.toString());
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;

        // Read the file line by line
        try {
            inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            // Skip the header row
            reader.readLine();
            // Read the rest of the records
            while ((line = reader.readLine()) != null) {
                // Use a more robust CSV parsing method
                Track fields = parseCSVLine(line);
                records.add(fields);
            }
        } finally {
            // Close the reader and the input stream
            IOUtils.closeStream(reader);
            IOUtils.closeStream(inputStream);
        }
        System.out.println("\033[1;92mFile parsed.\033[0m");
        return records;
    }

    /**
     * Parse a line of CSV data.
     * @param line
     * @return A Track object.
     */
    public Track parseCSVLine(String line) {
        // Create a list to store the fields
        List<String> values = new ArrayList<>();

        // Create a string builder to store the current field
        StringBuilder currentValue = new StringBuilder();
        // Create a flag to keep track of whether we're inside quotes
        boolean inQuotes = false;

        // Iterate over the characters in the line
        for (char c : line.toCharArray()) {
            // Check if the character is a quote
            if (c == '"') {
                // If it is, toggle the inQuotes flag
                inQuotes = !inQuotes; // Toggle the inQuotes flag
            } else if (c == ',' && !inQuotes) {
                // If we encounter a comma and we're not inside quotes, it's the end of a field
                values.add(currentValue.toString());
                currentValue.setLength(0); // Clear the current field
            } else {
                // Otherwise, just add the character to the current field
                currentValue.append(c);
            }
        }
        // Add the last field
        values.add(currentValue.toString());

        // Create a new Track object
        Track track;
        try {
            track = new Track(CSV_HEADER, values);
        } catch (Exception e) {
            // If there's an error, print the line and the fields
            System.out.println("Error parsing line: " + line);
            System.out.println("Fields: " + values);
            e.printStackTrace();
            System.exit(1);
            return null;
        }

        // Return the fields as a Track object
        return track;
    }

    /**
     * Convert a list of tracks to a Weka dataset.
     * @param tracks
     * @param target
     * @return A Weka dataset.
     */
    public Instances trackToDataset(List<Track> tracks, String target) {
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

        // Create the dataset
        Instances dataset = new Instances("tracks", attributes, tracks.size());
        dataset.setClassIndex(attributes.size() - 1);

        // Add the tracks to the dataset as instances
        for (Track track : tracks) {
            double[] values = new double[attributes.size()];
            values[0] = track.getAcousticness();
            values[1] = track.getDanceability();
            values[2] = track.getEnergy();
            values[3] = track.getInstrumentalness();
            values[4] = track.getLiveness();
            values[5] = track.getLoudness();
            values[6] = track.getSpeechiness();
            values[7] = track.getTempo();
            values[8] = track.getValence();
            values[9] = track.getDuration();
            values[10] = track.getExplicit();
            values[11] = track.getKey();
            values[12] = track.getMode();
            values[13] = track.getTimeSignature();
            values[14] = track.getFollowers();
            values[15] = track.getAlbumPopularity();
            values[16] = track.getArtistPopularity();
            values[17] = classValues.indexOf(String.valueOf(track.getTarget(target)));

            dataset.add(new DenseInstance(1.0, values));
        }

        return dataset;
    }

    public Instances arrayToDataset(List<double[]> data, String target) {
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

        // Create the dataset
        Instances dataset = new Instances("tracks", attributes, data.size());
        dataset.setClassIndex(attributes.size() - 1);

        // Add the tracks to the dataset as instances
        for (double[] values : data) {
            dataset.add(new DenseInstance(1.0, values));
        }


        return dataset;
    }

}