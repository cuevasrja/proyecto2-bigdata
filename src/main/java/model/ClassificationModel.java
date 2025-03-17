package model;

import java.util.List;

import parser.SpotifyCsvParser;
import spotify.Track;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;

public class ClassificationModel {
    private static String target;
    private NaiveBayes classifier = new NaiveBayes();
    private SpotifyCsvParser parser = new SpotifyCsvParser();
    private Evaluation eval;

    /**
     * Constructor for the ClassificationModel.
     * @param target The target attribute.
     */
    public ClassificationModel(String target) {
        ClassificationModel.target = target;
    }

    /**
     * Train the model with the given tracks.
     * @param tracks The tracks to train the model with.
     * @throws Exception If the model cannot be trained.
     */
    public void train(List<Track> tracks) throws Exception {
        Instances dataset = parser.trackToDataset(tracks, target);
        dataset.setClassIndex(dataset.numAttributes() - 1);
        classifier.buildClassifier(dataset);
    }

    /**
     * Train the model with the given dataset.
     * @param dataset The dataset to train the model with.
     * @throws Exception If the model cannot be trained.
     */
    public void train(Instances dataset) throws Exception {
        dataset.setClassIndex(dataset.numAttributes() - 1);
        classifier.buildClassifier(dataset);
    }

    /**
     * Predict the target attribute of the given tracks.
     * @param tracks The tracks to predict.
     * @return The predictions.
     * @throws Exception If the predictions cannot be made.
     */
    public double[] predict(List<Track> tracks) throws Exception {
        Instances dataset = parser.trackToDataset(tracks, target);
        eval = new Evaluation(dataset);
        return eval.evaluateModel(classifier, dataset);
    }

    /**
     * Predict the target attribute of the given dataset.
     * @param dataset The dataset to predict.
     * @return The predictions.
     * @throws Exception If the predictions cannot be made.
     */
    public double[] predict(Instances dataset) throws Exception {
        eval = new Evaluation(dataset);
        return eval.evaluateModel(classifier, dataset);
    }

    /**
     * Print the metrics of the model.
     * @throws Exception
     */
    public void printMetrics() {
        // Print the metrics
        System.out.println(eval.errorRate());
        System.out.println(eval.toSummaryString());

        // Get the metrics
        List<weka.classifiers.evaluation.Prediction> metrics = eval.predictions();

        // Count the correct and incorrect predictions
        int correct = 0;
        int incorrect = 0;

        // For each metric, get the actual and predicted values
        for (weka.classifiers.evaluation.Prediction metric : metrics) {
            double actual = metric.actual();
            double predicted = metric.predicted();

            // If the actual and predicted values are the same, increment the correct count
            if (actual == predicted) {
                correct++;
            // Otherwise, increment the incorrect count and print the actual and predicted values
            } else {
                System.out.println("Actual: " + actual + " Predicted: " + predicted);
                incorrect++;
            }
        }
        
        // Print the correct and incorrect counts
        System.out.println("Correct: " + correct + " Incorrect: " + incorrect);
    }

}
