package spotify;

import java.util.Map;
import com.google.common.collect.Maps;

import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*
 {"namespace": "classes.avro",
 "type": "record",
 "name": "spotify",
 "fields": [
     {"name": "id", "type": "string"},
     {"name": "track_name",  "type": "string"},
     {"name": "disc_number", "type": "int"},
     {"name": "duration", "type": "int"},
     {"name": "explicit", "type": "int"},
     {"name": "audio_feature_id", "type": ["string", "null"]},
     {"name": "preview_url", "type": ["string", "null"]},
     {"name": "track_number", "type": ["int", "string", "null"]},
     {"name": "popularity", "type": "int"},
     {"name": "is_playable", "type": "int"},
     {"name": "acousticness", "type": "float"},
     {"name": "danceability", "type": "float"},
     {"name": "energy", "type": "float"},
     {"name": "instrumentalness", "type": "float"},
     {"name": "key", "type": ["int", "string", "null"]},
     {"name": "liveness", "type": "float"},
     {"name": "loudness", "type": "float"},
     {"name": "mode", "type": "int"},
     {"name": "speechiness", "type": "float"},
     {"name": "tempo", "type": "float"},
     {"name": "time_signature", "type": "int"},
     {"name": "valence", "type": "float"},
     {"name": "album_name", "type": ["string", "null"]},
     {"name": "album_group", "type": ["string", "null"]},
     {"name": "album_type", "type": ["string", "null"]},
     {"name": "release_date", "type": ["string", "null"]},
     {"name": "album_popularity", "type": ["int", "null"]},
     {"name": "artist_name", "type": "string"},
     {"name": "artist_popularity", "type": "int"},
     {"name": "followers", "type": "int"},
     {"name": "genre_id", "type": ["string", "null"]}
 ]
}
 */
public class Track {
    public static final int FEATURES = 17;
    public static final int TARGETS = 1;

    private List<Double> trackFeatures = new ArrayList<Double>(FEATURES);
    private List<Double> targetList = new ArrayList<Double>(TARGETS);

    private Map<String, String> fields = Maps.newLinkedHashMap();

    public Track(Iterable<String> fieldNames, Iterable<String> values) {
        Iterator<String> valueIterator = values.iterator();
        for (String fieldName : fieldNames) {
            String value = valueIterator.next();
            fields.put(fieldName, value);

            switch (fieldName){
                case "id":
                case "track_name":
                case "artist_name":
                case "genre_id":
                    // featureEncoder.addToVector(fieldName + ":" +value, 1.0, featuresVector);
                    break;
                case "audio_feature_id":
                case "preview_url":
                case "album_name":
                case "album_group":
                case "album_type":
                case "release_date":
                case "track_number":
                case "disc_number":
                    // featureEncoder.addToVector(value != null ? fieldName + ":" + value : fieldName, 0.0, featuresVector);
                    break;
                case "duration":
                case "explicit":
                case "is_playable":
                case "key":
                case "mode":
                case "time_signature":
                case "followers":
                    Double v = value != null && !value.equals("") ? Double.parseDouble(value) : 0.0;
                    trackFeatures.add(v);
                    break;
                case "acousticness":
                case "danceability":
                case "energy":
                case "instrumentalness":
                case "liveness":
                case "loudness":
                case "speechiness":
                case "tempo":
                case "valence":
                    Double f = value != null && !value.equals("") ? Double.parseDouble(value) : 0.0f;
                    trackFeatures.add(f);
                    break;
                case "popularity":
                case "album_popularity":
                case "artist_popularity":
                    Double t = value != null && !value.equals("") ? Double.parseDouble(value) : 0;
                    targetList.add(t);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field name: " + fieldName);
            }
        }
    }

    public int getCategory(String target) {
        return Integer.parseInt(fields.get(target));
    }

    public List<Double> getTrackFeatures() {
        return trackFeatures;
    }

    public Double[] getFeaturesArray() {
        Double[] features = new Double[FEATURES];
        for (int i = 0; i < FEATURES; i++) {
            features[i] = trackFeatures.get(i);
        }
        return features;
    }

    public double getAcousticness() {
        return fields.get("acousticness") != null ? Double.parseDouble(fields.get("acousticness")) : 0.0;
    }

    public double getDanceability() {
        return fields.get("danceability") != null ? Double.parseDouble(fields.get("danceability")) : 0.0;
    }

    public double getEnergy() {
        return fields.get("energy") != null ? Double.parseDouble(fields.get("energy")) : 0.0;
    }

    public double getInstrumentalness() {
        return fields.get("instrumentalness") != null ? Double.parseDouble(fields.get("instrumentalness")) : 0.0;
    }

    public double getSpeechiness() {
        return fields.get("speechiness") != null ? Double.parseDouble(fields.get("speechiness")) : 0.0;
    }

    public double getLiveness() {
        return fields.get("liveness") != null ? Double.parseDouble(fields.get("liveness")) : 0.0;
    }

    public double getLoudness() {
        return fields.get("loudness") != null ? Double.parseDouble(fields.get("loudness")) : 0.0;
    }

    public double getTempo() {
        return fields.get("tempo") != null ? Double.parseDouble(fields.get("tempo")) : 0.0;
    }

    public double getValence() {
        return fields.get("valence") != null ? Double.parseDouble(fields.get("valence")) : 0.0;
    }

    public double getDuration() {
        return fields.get("duration") != null ? Double.parseDouble(fields.get("duration")) : 0.0;
    }

    public double getExplicit() {
        return fields.get("explicit") != null ? Double.parseDouble(fields.get("explicit")) : 0.0;
    }

    public double getKey() {
        return fields.get("key") != null ? Double.parseDouble(fields.get("key")) : 0.0;
    }

    public double getTimeSignature() {
        return fields.get("time_signature") != null ? Double.parseDouble(fields.get("time_signature")) : 0.0;
    }

    public double getMode() {
        return fields.get("mode") != null ? Double.parseDouble(fields.get("mode")) : 0.0;
    }

    public double getAlbumPopularity() {
        return fields.get("album_popularity") != null && !fields.get("album_popularity").equals("") ? Double.parseDouble(fields.get("album_popularity")) : 0.0;
    }

    public double getFollowers() {
        return fields.get("followers") != null ? Double.parseDouble(fields.get("followers")) : 0.0;
    }

    public double getArtistPopularity() {
        return fields.get("artist_popularity") != null ? Double.parseDouble(fields.get("artist_popularity")) : 0.0;
    }


    public Instance toInstance(Instances dataset, String target) {
        Instance instance = new DenseInstance(FEATURES+1);
        instance.setDataset(dataset);
        for (int i = 0; i < FEATURES; i++) {
            instance.setValue(i, trackFeatures.get(i));
        }
        instance.setValue(FEATURES, this.getCategory(target));
        
        return instance;
    }

}
