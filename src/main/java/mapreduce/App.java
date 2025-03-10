package mapreduce;

// import com.google.common.collect.Lists;
// import org.apache.mahout.classifier.evaluation.Auc;
// import org.apache.mahout.classifier.sgd.L1;
// import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;

// import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;

import parser.SpotifyCsvParser;
import spotify.Track;
import model.LogisticRegression;

public class App {
    private static double[] ALPHA = {
        1.0,
        0.5,
        0.1,
        0.05,
        0.01,
        0.005,
        0.001,
        0.0005,
        0.0001
    };
    private static double[] LAMBDA = {
        100.0,
        50.0,
        10.0,
        5.0,
        1.0,
        0.5,
        0.1,
        0.05,
        0.01
    };
    private static int[] STEP_OFFSET = {
        1000,
        500,
        100,
        50,
        10,
        5,
        1,
        0
    };
    private static float[] DECAY_EXPO = {
        2.0f,
        1.0f,
        0.5f,
        0.1f,
        0.05f,
        0.01f,
        0.005f,
        0.001f,
        0.0005f
    };
    private static float[] LEARNING_RATE = {
        10.0f,
        5.0f,
        1.0f,
        0.5f,
        0.1f,
        0.05f,
        0.01f,
        0.005f,
        0.001f
    };
    private static int NUM_CATEGORIES = 100;
    public static void main( String[] args ){
        // if (args.length < 1) {
        //     System.out.println("Usage: App <file> [target]");
        //     System.exit(1);
        // }
        Text file = new Text("./tracks/tracks-medium.csv");
        String target = args.length > 1 ? args[1] : "popularity";
        SpotifyCsvParser parser = new SpotifyCsvParser();
        try {
            List<Track> tracks = parser.parse(file);
            float max_auc = 0.0f;
            String best_params = "";
            for (double alpha : ALPHA) {
                for (double lambda : LAMBDA) {
                    for (int stepOffset : STEP_OFFSET) {
                        for (float decayExpo : DECAY_EXPO) {
                            for (float learningRate : LEARNING_RATE) {
                                System.out.println("Alpha: " + alpha + ", Lambda: " + lambda + ", Step Offset: " + stepOffset + ", Decay Exponent: " + decayExpo + ", Learning Rate: " + learningRate);
                                LogisticRegression model = new LogisticRegression(target, alpha, lambda, stepOffset, decayExpo, learningRate, NUM_CATEGORIES);
                                model.train(tracks);
                                float auc = model.test(tracks);
                                if (auc > max_auc) {
                                    max_auc = auc;
                                    best_params = "Alpha: " + alpha + ", Lambda: " + lambda + ", Step Offset: " + stepOffset + ", Decay Exponent: " + decayExpo + ", Learning Rate: " + learningRate;
                                }
                                System.out.println("AUC: " + auc);
                                model.close();
                            }
                        }
                    }
                }
            }
            System.out.println("Best parameters: " + best_params);
            System.out.println("Max AUC: " + max_auc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
