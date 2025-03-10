package model;

import java.util.List;

import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;

import spotify.Track;

public class LogisticRegression implements AutoCloseable {
    private static final int RUNS = 100;
    private static double ALPHA = 1.0;
    private static double LAMBDA = 100.0;
    private static int STEP_OFFSET = 1000;
    private static float DECAY_EXPO = 2.0f;
    private static float LEARNING_RATE = 10.0f;
    private static int NUM_CATEGORIES = 100;
    private static OnlineLogisticRegression model;
    private static String target;

    public LogisticRegression(String target, double alpha, double lambda, int stepOffset, float decayExpo, float learningRate, int numCategories) {
        if (target == null || target.isEmpty()) {
            target = "popularity";
        }
        ALPHA = alpha;
        LAMBDA = lambda;
        STEP_OFFSET = stepOffset;
        DECAY_EXPO = decayExpo;
        LEARNING_RATE = learningRate;
        NUM_CATEGORIES = numCategories;
        try (OnlineLogisticRegression tempModel = new OnlineLogisticRegression(NUM_CATEGORIES, Track.FEATURES, new L1())) {
            tempModel.learningRate(LEARNING_RATE)
                .alpha(ALPHA)
                .lambda(LAMBDA)
                .stepOffset(STEP_OFFSET)
                .decayExponent(DECAY_EXPO);
            model = tempModel;
        }
        LogisticRegression.target = target;
    }

    public LogisticRegression(String target) {
        if (target == null || target.isEmpty()) {
            target = "popularity";
        }
        try (OnlineLogisticRegression tempModel = new OnlineLogisticRegression(NUM_CATEGORIES, Track.FEATURES, new L1())) {
            tempModel.learningRate(LEARNING_RATE)
                .alpha(ALPHA)
                .lambda(LAMBDA)
                .stepOffset(STEP_OFFSET)
                .decayExponent(DECAY_EXPO);
            model = tempModel;
        }
        LogisticRegression.target = target;
    }

    public void train(List<Track> tracks) {
        // System.out.println("\033[1;94mTraining model...\033[0m");
        for (int run = 1; run <= RUNS; run++) {
            // System.out.println("\033[94mRun " + run + "...\033[0m");
            for (Track track : tracks) {
                int category = track.getCategory(target);
                // System.out.println("Training with category: " + category + " and features: " + track.getFeatures());
                model.train(category, track.getFeatures());
            }
        }
        // System.out.println("\033[1;92mModel trained.\033[0m");
    }
    
    public float test(List<Track> tracks) {
        int correct = 0;
        int total = 0;
        for (Track track : tracks) {
            int target = track.getCategory(LogisticRegression.target);
            // System.out.println("Target: " + target);
            int predicted = model.classifyFull(track.getFeatures()).maxValueIndex();
            // System.out.println("Predicted: " + predicted);
            if (target == predicted) {
                correct++;
            }
            total++;
        }
        float accuracy = correct / (float) total;
        System.out.println("Correct: " + correct + " Total: " + total);
        System.out.println("Accuracy: " + accuracy);
        return accuracy;
    }

    public void close() {
        model.close();
    }

    public int predict(Track track) {
        return model.classify(track.getFeatures()).maxValueIndex();
    }
    
}
