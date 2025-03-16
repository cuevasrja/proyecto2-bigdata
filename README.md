# Proyecto 2 - Big Data

## Integrantes

- **Laura León** - 17-10307
- **Juan Cuevas** - 19-10056
- **Anya Marcano** - 19-10336

## Instrucciones

Primeramente hay que instalar y compilar el proyecto. Para ello, se debe correr el siguiente comando en la terminal para eliminar cualquier archivo compilado previamente y compilar el proyecto:

```bash
mvn clean install
```

Luego, se instalará y compilará el proyecto. Para correr el proyecto, se debe correr el siguiente comando en la terminal:

```bash
mvn install && mvn compile
```

Finalmente, para correr el proyecto, se debe correr el siguiente comando en la terminal:

```bash
hadoop jar target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar App <n> <input> <output>
```

Donde:

- `<n>` es el caso de uso que se desea ejecutar.
- `<input>` es el archivo de entrada.
- `<output>` es el archivo de salida.

Ejemplo:

```bash
hadoop jar target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar App 1 tracks/tracks_n_200.csv out/
```

Para ejecutar el proyecto localmente, se debe correr el siguiente comando en la terminal:

```bash
mvn exec:java -Dexec.mainClass="App" -Dexec.args="<n> <input> <output>"
```
