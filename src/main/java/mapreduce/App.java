package mapreduce;

// import com.google.common.collect.Lists;
// import org.apache.mahout.classifier.evaluation.Auc;
// import org.apache.mahout.classifier.sgd.L1;
// import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;

// import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;

import model.ClassificationModel;
import parser.SpotifyCsvParser;
import spotify.Track;

public class App {
      public static void main(String[] args) throws Exception {
        // if (args.length < 1) {
        //     System.out.println("Usage: App <file> [target]");
        //     System.exit(1);
        // }
        Text file = new Text("./tracks/tracks_n_200.csv");
        String target = args.length > 1 ? args[1] : "popularity";
        SpotifyCsvParser parser = new SpotifyCsvParser();
        ClassificationModel model = new ClassificationModel(target);
        try {
            List<Track> tracks = parser.parse(file);
            model.train(tracks);
            model.predict(tracks);
            model.printMetrics();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
      }
}

