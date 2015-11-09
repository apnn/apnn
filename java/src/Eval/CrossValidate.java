package Eval;

import Eval.Metric.Document;
import Eval.Metric.SuspiciousDocument;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapMap;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.io.Datafile;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Simulated n-fold cross validation over a set of result files per mixture of 
 * parameter settings, choosing for each fold the optimal settings using the
 * remaining (n-1) folds, and repeating the process for all folds.
 * @author Jeroen
 */
public class CrossValidate {
    public static Log log = new Log(CrossValidate.class);
    Metric metric;
    int k;
    HashMap<String, HashMap<Document, Double>> parameterResults = new HashMap();
    
    public CrossValidate(Metric metric, int k) {
        this.metric = metric;
        this.k = k;
    }
    
    /**
     * Add the resultsFile of a parameter setting
     * @param parameterSetting
     * @param resultsFile file in SimilarityFile format containing the results for the
     * given parameterSetting
     */
    public void addParameterResults(String parameterSetting, Datafile resultsFile) {
        Metric.ResultSet retrievedDocuments = Metric.loadFile(resultsFile);
        HashMap<Document, Double> scores = metric.score(retrievedDocuments, k);
        parameterResults.put(parameterSetting, scores);
    }
    
    /**
     * Cross-validates using n-folds, for each fold (the test fold) using the remaining (n-1)
     * folds (the training set for the test fold) to find the parameter setting
     * that maximizes the effectiveness over the training set and then use that
     * parameter setting for the test fold. The result is the 
     * settings over the remaining (n-1) folds
     * @param n
     * @return 
     */
    public double crossValidate(int n) {
        FHashSet<SuspiciousDocument>[] folds = createFolds(n);
        ArrayList<Double> scores = new ArrayList();
        for (int i = 0; i < n; i++) {
            ArrayMap<Double, String> trainedScores = getTrainedScores(folds[i]);
            HashMapMap<String, Document, Double> testScores = getTestScoreMap(folds[i]);
            String selectedParameter = trainedScores.getValue(0);
            Map<Document, Double> foldScores  = testScores.get(selectedParameter);
            log.printf("fold %d param %s", i, selectedParameter);
            scores.addAll(foldScores.values());
        }
        return DoubleTools.mean(scores);
    }
    
    
    /**
     * @param testSet the documents that can not be used for training
     * @return An ArrayMap of the average score per parameter over all documents
     * outside the testSet (i.e. the training set)
     */
    public ArrayMap<Double, String> getTrainedScores(FHashSet<SuspiciousDocument> testSet) {
        ArrayMap<Double, String> trainedScorePerParameter = new ArrayMap();
        for (Map.Entry<String, HashMap<Document, Double>> parameterResult : parameterResults.entrySet()) {
            String parameter = parameterResult.getKey();
            HashMap<Document, Double> results = parameterResult.getValue();
            ArrayList<Double> trainingScores = new ArrayList();
            for (Map.Entry<Document, Double> result : results.entrySet()) {
                Document document = result.getKey();
                Double score = result.getValue();
                if (!testSet.contains(document)) {
                    trainingScores.add(score);
                }
            }
            double meanTrainingScore = DoubleTools.mean(trainingScores);
            trainedScorePerParameter.add(meanTrainingScore, parameter);
        }
        return trainedScorePerParameter.descending();
    }

    /**
     * @param testSet a set of suspicious documents to retrieve the scores for.
     * @return A Map with per parameter a Map of the score per suspicousDocumentId
     * of all the subset of documents.
     */
    public HashMapMap<String, Document, Double> getTestScoreMap(FHashSet<SuspiciousDocument> testSet) {
        HashMapMap<String, Document, Double> testScorePerParameter = new HashMapMap();
        for (Map.Entry<String, HashMap<Document, Double>> parameterResult : parameterResults.entrySet()) {
            String parameter = parameterResult.getKey();
            HashMap<Document, Double> results = parameterResult.getValue();
            HashMap<Document, Double> testScores = new HashMap();
            for (Map.Entry<Document, Double> result : results.entrySet()) {
                Document document = result.getKey();
                Double score = result.getValue();
                if (testSet.contains(document)) {
                    testScores.put(document, score);
                }
            }
            testScorePerParameter.put(parameter, testScores);
        }
        return testScorePerParameter;
    }
    
    /**
     * Splits the test set into n-folds, by simply modding the docids over 
     * the folds, assuming that the docids are distributing evenly over space
     * the folds should have close to the same number of documents and are
     * not picked sequentially.
     * @param n number of folds
     * @return a split of the ground truth documents into n-folds.
     */
    public FHashSet<SuspiciousDocument>[] createFolds(int n) {
        FHashSet<SuspiciousDocument>[] folds = new FHashSet[n];
        for (int i = 0; i < n; i++)
            folds[i] = new FHashSet();
        for (SuspiciousDocument suspiciousDocument : metric.getGroundTruth().values()) {
            int fold = suspiciousDocument.docid % n;
            folds[fold].add(suspiciousDocument);
        }
        return folds;
    }
    
    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "groundtruth rank -r {results} -f [folds]");
        Datafile gtFile = conf.getHDFSFile("groundtruth");
        int rank = conf.getInt("rank", 100);
        int folds = conf.getInt("folds", 10);
        NDCG ndcg = new NDCG(gtFile);
        CrossValidate crossValidate = new CrossValidate(ndcg, rank);
        for (String resultFilename : conf.getStrings("results")) {
            Datafile resultFile = new Datafile(conf, resultFilename);
            crossValidate.addParameterResults(resultFile.getName(), resultFile);
        }
        double meanScore = crossValidate.crossValidate(folds);
        log.printf("Cross-validated over %d-folds average nDCG@%d=%f", 
                folds, rank, meanScore);
    }
}
