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
import java.util.List;
import spotify.Track;

public class SpotifyCsvParser {

    /**
     * Parse the records in a text file.
     * @param value
     * @return A list of records.
     * @throws IOException
     */
    public List<Track> parse(Text value) throws IOException {
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
        return records;
    }

    /**
     * Parse a line of CSV data.
     * @param line
     * @return An array of fields.
     */
    private Track parseCSVLine(String line) {
        // Create a list to store the fields
        List<String> fields = new ArrayList<>();

        // Create a string builder to store the current field
        StringBuilder currentField = new StringBuilder();
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
                fields.add(currentField.toString());
                currentField.setLength(0); // Clear the current field
            } else {
                // Otherwise, just add the character to the current field
                currentField.append(c);
            }
        }
        // Add the last field
        fields.add(currentField.toString());

        // Create a new Track object
        Track track;
        try {
            track = new Track(
                fields.get(0),
                fields.get(1),
                fields.get(2) != null ? Integer.parseInt(fields.get(2)) : 0,
                Integer.parseInt(fields.get(3)),
                Integer.parseInt(fields.get(4)),
                fields.get(5) != null ? fields.get(5) : "",
                fields.get(6) != null ? fields.get(6) : "",
                Integer.parseInt(fields.get(7)),
                Integer.parseInt(fields.get(8)),
                fields.get(9) != null ? Integer.parseInt(fields.get(9)) : 0,
                Float.parseFloat(fields.get(10)),
                Float.parseFloat(fields.get(11)),
                Float.parseFloat(fields.get(12)),
                Float.parseFloat(fields.get(13)),
                Integer.parseInt(fields.get(14)),
                Float.parseFloat(fields.get(15)),
                Float.parseFloat(fields.get(16)),
                Integer.parseInt(fields.get(17)),
                Float.parseFloat(fields.get(18)),
                Float.parseFloat(fields.get(19)),
                Integer.parseInt(fields.get(20)),
                Float.parseFloat(fields.get(21)),
                fields.get(22) != null ? fields.get(22) : "",
                fields.get(23) != null ? fields.get(23) : "",
                fields.get(24) != null ? fields.get(24) : "",
                fields.get(25) != null ? fields.get(25) : "",
                fields.get(26) != null ? Integer.parseInt(fields.get(26)) : 0,
                fields.get(27),
                Integer.parseInt(fields.get(28)),
                Integer.parseInt(fields.get(29)),
                fields.get(30)
            );
        } catch (Exception e) {
            // If there's an error, print the line and the fields
            System.out.println("Error parsing line: " + line);
            System.out.println("Fields: " + fields);
            e.printStackTrace();
            return null;
        }

        // Return the fields as a Track object
        return track;
    }
}