package Eval;

import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import java.io.IOException;
import java.util.HashMap;

/**
 * Evaluates the retrieved approximate nearest neighbors using NDCG vs the
 * actual top-k most similar documents. To penalize missing the nearest document
 * more than the next-to-nearest, the actual top-k nearest documents receive
 * relevance grades (k+1-position).
 *
 * @author Jeroen
 */
public class NDCG extends Metric {

    public static Log log = new Log(NDCG.class);

    public NDCG(Datafile groundtruthFile, int k) {
        super(groundtruthFile, k);
    }

    @Override
    public double score(SuspiciousDocument groundtruth, SuspiciousDocument retrievedDocument) {
        return dcg(groundtruth, retrievedDocument) / dcg(groundtruth, groundtruth);
    }

    /**
     * @param optimalResult
     * @param retrievedResult
     * @return the Discounted Cumulative Gain for the retrieved NNs vs the
     * optimal NNs (ground truth) for the given suspicious document.
     */
    private double dcg(SuspiciousDocument optimalResult, SuspiciousDocument retrievedResult) {
        double dcg = 0;
        for (SourceDocument document : retrievedResult.relevantDocuments.values()) {
            // positions count from 0
            if (document.position < getK()) {
                SourceDocument gt = optimalResult.relevantDocuments.get(document.docid);
                if (gt != null) {
                    if (document.position == 0) {
                        dcg += gt.relevanceGrade;
                    } else {
                        dcg += gt.relevanceGrade / MathTools.log2(document.position + 1);
                    }
                }
            }
        }
        return dcg;
    }

    @Override
    protected int getNextRelevanceGrade(SuspiciousDocument suspiciousDoucment,
            double score) {
        return getK() - suspiciousDoucment.relevantDocuments.size();
    }

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "groundtruth results -k [k] --hdfs");

        Datafile groundtruth = conf.getBoolean("hdfs", false)
                ? conf.getHDFSFile("groundtruth")
                : conf.getFSFile("groundtruth");
        int k = conf.getInt("k", 10);
        NDCG metric = new NDCG(groundtruth, k);
        HPath path = conf.getBoolean("hdfs", false)
                ? conf.getHDFSPath("results")
                : conf.getFSPath("results");
        for (Datafile resultFile : path.getFiles()) {
            HashMap<Document, Double> score = metric.score(resultFile);
            double ndcg = metric.mean(score);
            log.printf("%s n=%d ndcg=%f", resultFile.getName(), score.size(), ndcg);
        }
    }
}
