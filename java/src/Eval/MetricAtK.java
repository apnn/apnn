package Eval;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.fcollection.FHashMap;
import io.github.htools.fcollection.FHashMapIntObject;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A generic class to compute an evaluation metric over a set of retrieved
 * nearest neighbor source documents for the collection of suspicious documents.
 * For each suspicious document the evaluation metric is based on the comparison
 * between the retrieved set of k-most nearest neighbors and a ground truth set
 * of k-most nearest neighbors. Both the retrieved set and ground truth should
 * be input as a SimilarityFile. When constructing a single ground truth file is
 * given, along with k which is the maximum rank considered (top-k).
 *
 * Important! The SimilarityFiles should be in order of descending similarity
 * score per suspicious document.
 *
 * @author Jeroen
 */
public abstract class MetricAtK extends Metric {

    public static Log log = new Log(MetricAtK.class);

    public MetricAtK(GTMap gt) {
        super(gt);
    }

    /**
     * @param groundtruth the optimal retrieved set of nearest neighbors for a
     * given suspicious document
     * @param retrievedDocument the retrieved set of nearest neighbors for a
     * given suspicious document
     * @return the metrics score for the nearest neighbors for the retrieved
     * document vs the ground truth
     */
    public abstract double score(GTQuery groundtruth,
            ResultQuery retrievedDocument, int k);

    /**
     * @param retrievedDocument
     * @return the score for a single document with its retrieved nearest
     * neighbors
     */
    public double score(ResultQuery retrievedDocument, int k) {
        GTQuery gt = groundTruth.get(retrievedDocument.queryid);
        if (gt != null && gt.relevantDocuments.size() > 0) {
            return score(gt, retrievedDocument, k);
        } else {
            return 0;
        }
    }

    /**
     * @param resultFile
     * @return a map of scored documents (id, score) for the retrieved nearest
     * neighbors in the given file.
     */
    public HashMap<Document, Double> score(ResultSet retrievedDocuments, int k) {
        HashMap<Document, Double> results = new HashMap();
        for (GTQuery query : this.groundTruth.values()) {
            ResultQuery scored = retrievedDocuments.get(query.queryid);
            if (scored != null) {
                double score = MetricAtK.this.score(scored, k);
                //log.info("%s score %f", Metric.this.getClass().getCanonicalName(), score);
                results.put(scored, score);
            }
        }
        return results;
    }
}
