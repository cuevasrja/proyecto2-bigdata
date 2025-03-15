import sqlite3
# import pandas as pd
import fireducks.pandas as pd
import sys
import os

# Create a connection to the database
conn = sqlite3.connect('spotify.sqlite')
cursor = conn.cursor()

# Load the schema using the .schema method
# schema = cursor.execute("SELECT sql, name FROM sqlite_master WHERE type='table'").fetchall()

# for table in schema:
#     print(table[0])
#     n_rows = pd.read_sql_query(f"SELECT COUNT(*) AS len FROM {table[1]};", conn)['len'][0]
#     print(f"Number of rows: {n_rows}")
#     print()

"""
CREATE TABLE albums (
    "id" TEXT PRIMARY KEY,
    "name" TEXT,
    "album_group" TEXT,
    "album_type" TEXT,
    "release_date" TEXT,
    "popularity" INTEGER
)
Number of rows: 4820754

CREATE TABLE artists (
    "name" TEXT,
    "id" TEXT PRIMARY KEY,
    "popularity" INTEGER,
    "followers" INTEGER
)
Number of rows: 1066031

CREATE TABLE audio_features (
    "id" TEXT PRIMARY KEY,
    "acousticness" REAL,
    "analysis_url" TEXT,
    "danceability" REAL,
    "duration" INTEGER,
    "energy" REAL,
    "instrumentalness" REAL,
    "key" INTEGER,
    "liveness" REAL,
    "loudness" REAL,
    "mode" INTEGER,
    "speechiness" REAL,
    "tempo" REAL,
    "time_signature" INTEGER,
    "valence" REAL
)
Number of rows: 8740043

CREATE TABLE genres (
    "id" TEXT PRIMARY KEY
)
Number of rows: 5489

CREATE TABLE r_albums_artists (
    "album_id" TEXT,
    "artist_id" TEXT,
    FOREIGN KEY ("album_id") REFERENCES "albums" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("artist_id") REFERENCES "artists" ("id") ON DELETE CASCADE
)
Number of rows: 921486

CREATE TABLE r_albums_tracks (
    "album_id" TEXT,
    "track_id" TEXT,
    FOREIGN KEY ("album_id") REFERENCES "albums" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("track_id") REFERENCES "_tracks_old" ("id") ON DELETE CASCADE
)
Number of rows: 9900173

CREATE TABLE r_artist_genre (
    "genre_id" TEXT,
    "artist_id" TEXT,
    FOREIGN KEY ("genre_id") REFERENCES "genres" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("artist_id") REFERENCES "artists" ("id") ON DELETE CASCADE
)
Number of rows: 487386

CREATE TABLE r_track_artist (
    "track_id" TEXT,
    "artist_id" TEXT,
    FOREIGN KEY ("track_id") REFERENCES "_tracks_old" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("artist_id") REFERENCES "artists" ("id") ON DELETE CASCADE
)
Number of rows: 11840402

CREATE TABLE tracks (
    "id" TEXT PRIMARY KEY,
    "disc_number" INTEGER,
    "duration" INTEGER,
    "explicit" INTEGER,
    "audio_feature_id" TEXT,
    "name" TEXT,
    "preview_url" TEXT,
    "track_number" INTEGER,
    "popularity" INTEGER,
    "is_playable" INTEGER,
    FOREIGN KEY ("audio_feature_id") REFERENCES "audio_features" ("id") ON DELETE CASCADE
)
Number of rows: 8741672
"""

# Si el número de argumentos es incorrecto, mostrar un mensaje de error
if len(sys.argv) != 3:
    print("\033[91mError: Número de argumentos incorrecto\033[0m")
    print("\033[93mUso\033[0m: python generator.py <flag>")
    print("\033[93mFlags\033[0m:")
    print("\033[93m-n\033[0m: Generar un archivo CSV con <n> tracks")
    print("\033[93m-p\033[0m: Generar un archivo CSV con el 100*p% de tracks")
    sys.exit(1)

n_tracks = pd.read_sql_query("SELECT COUNT(*) AS len FROM tracks;", conn)['len'][0]
filtered_tracks = 0

# Si el argumento es -n, generar un archivo CSV con n tracks
if sys.argv[1] == "-n":
    filtered_tracks = int(sys.argv[2])
    if filtered_tracks > n_tracks:
        print("\033[91mError: El número de tracks a generar es mayor al número de tracks en la base de datos\033[0m")
        sys.exit(1)
# Si el argumento es -p, generar un archivo CSV con el 100*p% de tracks
elif sys.argv[1] == "-p":
    p = float(sys.argv[2])
    if p > 1 or p < 0:
        print("\033[91mError: El porcentaje debe estar en el rango [0, 1]\033[0m")
        sys.exit(1)
    filtered_tracks = int(n_tracks * p)
else:
    print("\033[91mError: Argumento incorrecto\033[0m")
    sys.exit(1)

conn.text_factory = lambda b: b.decode(errors = 'ignore')

# Función para forzar la decodificación en UTF-8
def force_utf8(df):
    for col in df.select_dtypes(include=[object]).columns:
        df[col] = df[col].apply(lambda x: x.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore') if isinstance(x, str) else x)
    return df

print("Leer los datos de la base de datos")

print(f"Number of tracks: {n_tracks}")
print(f"Tracks filtered: {filtered_tracks}")

query = f"""SELECT
    t.id,
    t.name AS track_name,
    t.disc_number,
    t.duration,
    t.explicit,
    t.audio_feature_id,
    t.preview_url,
    t.track_number,
    t.popularity,
    t.is_playable,
    af.acousticness,
    af.danceability,
    af.energy,
    af.instrumentalness,
    af.key,
    af.liveness,
    af.loudness,
    af.mode,
    af.speechiness,
    af.tempo,
    af.time_signature,
    af.valence,
    albums.name AS album_name,
    albums.album_group,
    albums.album_type,
    albums.release_date,
    albums.popularity AS album_popularity,
    artists.name AS artist_name,
    artists.popularity AS artist_popularity,
    artists.followers,
    genres.genre_id AS genre_id
FROM tracks AS t
LEFT JOIN audio_features AS af ON t.audio_feature_id = af.id
LEFT JOIN r_albums_tracks AS rat ON t.id = rat.track_id
LEFT JOIN albums ON rat.album_id = albums.id
LEFT JOIN r_track_artist AS rta ON t.id = rta.track_id
LEFT JOIN artists ON rta.artist_id = artists.id
LEFT JOIN r_artist_genre AS genres ON artists.id = genres.artist_id
ORDER BY RANDOM()
LIMIT {filtered_tracks};"""

print("Query:")
print(query)
tracks = pd.read_sql_query(query, conn)
print("Force UTF-8")
tracks = force_utf8(tracks)

print(tracks.head())

output_file = f"tracks_{filtered_tracks}.csv"
print(f"Write to {output_file}")

# Si la carpeta tracks no existe, crearla
if not os.path.exists("tracks"):
    os.makedirs("tracks")

# Convertir la tabla a un archivo CSV y guardar en tracks/<output_file>
tracks.to_csv(f"tracks/{output_file}", index=False)

# Cerrar la conexión a la base de datos
conn.close()