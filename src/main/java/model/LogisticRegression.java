package model;

import java.util.List;

import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;

import spotify.Track;
import parser.SpotifyCsvParser;

public class LogisticRegression {
    private static final int RUNS = 10;
    private static final double ALPHA = 0.01;
    private static final double LAMBDA = 0.00001;
    private static final double THRESHOLD = 0.5;
    private static final int STEP_OFFSET = 1000;
    private static final float DECAY_EXPO = 0.2f;
    private static final float HELD_OUT_RATIO = 0.1f;
    private static final float LEARNING_RATE = 1.0f;
    private static final int NUM_CATEGORIES = 100;

    public static OnlineLogisticRegression trainModel(List<Track> tracks) {
        OnlineLogisticRegression model = new OnlineLogisticRegression(NUM_CATEGORIES, Track.FEATURES, new L1())
            .learningRate(LEARNING_RATE)
            .alpha(ALPHA)
            .lambda(LAMBDA)
            .stepOffset(STEP_OFFSET)
            .decayExponent(DECAY_EXPO);




        return model;
    }

    
}
