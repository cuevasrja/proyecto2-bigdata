package spotify;

import org.apache.hadoop.thirdparty.protobuf.Any;

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
    public String id;
    public String track_name;
    public int disc_number;
    public int duration;
    public int explicit;
    public String audio_feature_id;
    public String preview_url;
    public int track_number;
    public int popularity;
    public int is_playable;
    public float acousticness;
    public float danceability;
    public float energy;
    public float instrumentalness;
    public int key;
    public float liveness;
    public float loudness;
    public int mode;
    public float speechiness;
    public float tempo;
    public int time_signature;
    public float valence;
    public String album_name;
    public String album_group;
    public String album_type;
    public String release_date;
    public int album_popularity;
    public String artist_name;
    public int artist_popularity;
    public int followers;
    public String genre_id;


    public Track(String id, String track_name, int disc_number, int duration, int explicit, String audio_feature_id, String preview_url, int track_number, int popularity, int is_playable, float acousticness, float danceability, float energy, float instrumentalness, int key, float liveness, float loudness, int mode, float speechiness, float tempo, int time_signature, float valence, String album_name, String album_group, String album_type, String release_date, int album_popularity, String artist_name, int artist_popularity, int followers, String genre_id) {
        this.id = id;
        this.track_name = track_name;
        this.disc_number = disc_number;
        this.duration = duration;
        this.explicit = explicit;
        this.audio_feature_id = audio_feature_id;
        this.preview_url = preview_url;
        this.track_number = track_number;
        this.popularity = popularity;
        this.is_playable = is_playable;
        this.acousticness = acousticness;
        this.danceability = danceability;
        this.energy = energy;
        this.instrumentalness = instrumentalness;
        this.key = key;
        this.liveness = liveness;
        this.loudness = loudness;
        this.mode = mode;
        this.speechiness = speechiness;
        this.tempo = tempo;
        this.time_signature = time_signature;
        this.valence = valence;
        this.album_name = album_name;
        this.album_group = album_group;
        this.album_type = album_type;
        this.release_date = release_date;
        this.album_popularity = album_popularity;
        this.artist_name = artist_name;
        this.artist_popularity = artist_popularity;
        this.followers = followers;
        this.genre_id = genre_id;

    }
}
