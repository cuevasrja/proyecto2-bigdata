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

    public ClassificationModel(String target) {
        ClassificationModel.target = target;
    }

    public void train(List<Track> tracks) throws Exception {
        Instances dataset = parser.trackToDataset(tracks, target);
        dataset.setClassIndex(dataset.numAttributes() - 1);
        classifier.buildClassifier(dataset);
    }

    public void train(Instances dataset) throws Exception {
        dataset.setClassIndex(dataset.numAttributes() - 1);
        classifier.buildClassifier(dataset);
    }

    public double[] predict(List<Track> tracks) throws Exception {
        Instances dataset = parser.trackToDataset(tracks, target);
        eval = new Evaluation(dataset);
        return eval.evaluateModel(classifier, dataset);
    }

    public double[] predict(Instances dataset) throws Exception {
        eval = new Evaluation(dataset);
        return eval.evaluateModel(classifier, dataset);
    }

    public void printMetrics() {
        System.out.println(eval.errorRate());
        System.out.println(eval.toSummaryString());
        List<weka.classifiers.evaluation.Prediction> metrics = eval.predictions();
        int correct = 0;
        int incorrect = 0;
        for (weka.classifiers.evaluation.Prediction metric : metrics) {
            double actual = metric.actual();
            double predicted = metric.predicted();
            if (actual == predicted) {
                correct++;
            } else {
                System.out.println("Actual: " + actual + " Predicted: " + predicted);
                incorrect++;
            }
        }
        System.out.println("Correct: " + correct + " Incorrect: " + incorrect);
    }

}
