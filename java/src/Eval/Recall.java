package Eval;

import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.HashMap;

/**
 * Computes the recall, or in this case in the absence of binary labels the
 * fraction of items in the top-k of the ground truth that are retrieved in the
 * top-k using the approach.
 *
 * @author Jeroen
 */
public class Recall extends Metric {

    public static Log log = new Log(Recall.class);

    public Recall(Datafile groundtruthFile, int k) {
        super(groundtruthFile, k);
    }

    @Override
    public double score(SuspiciousDocument groundtruth, SuspiciousDocument retrievedDocument) {
        int countRetrievedTopK = 0;
        for (SourceDocument d : retrievedDocument.relevantDocuments.values()) {
            if (d.position < this.getK()) {
                SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.docid);
                if (groundTruthResult != null && groundTruthResult.position < getK()) {
                    countRetrievedTopK++;
                }
            }
        }
        return countRetrievedTopK / (double) Math.min(groundtruth.relevantDocuments.size(), getK());
    }

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "groundtruth results -k [k] --hdfs");

        Datafile groundtruth = conf.getBoolean("hdfs", false)
                ? conf.getHDFSFile("groundtruth")
                : conf.getFSFile("groundtruth");
        int k = conf.getInt("k", 10);
        Recall metric = new Recall(groundtruth, k);
        HPath path = conf.getBoolean("hdfs", false)
                ? conf.getHDFSPath("results")
                : conf.getFSPath("results");
        for (Datafile resultFile : path.getFiles()) {
            HashMap<Document, Double> score = metric.score(resultFile);
            double recall = metric.mean(score);
            log.printf("%s n=%d recall=%f", resultFile.getName(), score.size(), recall);
        }
    }

    @Override
    protected int getNextRelevanceGrade(SuspiciousDocument suspiciousDoucment, double score) {
        return 1;
    }
}
