package Eval;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.io.Datafile;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.util.HashMap;

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
public abstract class Metric {

    public static Log log = new Log(Metric.class);
    private int k;
    private HashMap<Integer, SuspiciousDocument> groundtruth;

    public Metric(Datafile groundtruthFile, int k) {
        this.k = k;
        groundtruth = loadFile(groundtruthFile);
    }

    /**
     * @param groundtruth the optimal retrieved set of nearest neighbors for a
     * given suspicious document
     * @param retrievedDocument the retrieved set of nearest neighbors for a
     * given suspicious document
     * @return the metrics score for the nearest neighbors for the retrieved
     * document vs the ground truth
     */
    public abstract double score(SuspiciousDocument groundtruth,
            SuspiciousDocument retrievedDocument);

    /**
     * @param retrievedDocument
     * @return the score for a single document with its retrieved nearest
     * neighbors
     */
    public double score(SuspiciousDocument retrievedDocument) {
        SuspiciousDocument gt = groundtruth.get(retrievedDocument.docid);
        if (gt != null) {
            return score(gt, retrievedDocument);
        } else {
            return 0;
        }
    }

    /**
     * @return k as in metric@k to restrict the top-k ranks considered. 
     */
    public int getK() {
        return k;
    }
    
    /**
     * @param resultFile
     * @return a map of scored documents (id, score) for the retrieved nearest
     * neighbors in the given file.
     */
    public HashMap<Integer, Double> score(Datafile resultFile) {
        HashMap<Integer, Double> results = new HashMap();
        HashMap<Integer, SuspiciousDocument> scoredDocuments = loadFile(resultFile);
        for (SuspiciousDocument scored : scoredDocuments.values()) {
            double score = score(scored);
            results.put(scored.docid, score);
        }
        return results;
    }

    /**
     * @param scores map of scored documents (id, score)
     * @return the mean metric over the scored documents
     */
    public double mean(HashMap<Integer, Double> scores) {
        return DoubleTools.mean(scores.values());
    }

    /**
     * Load the ground truth file
     *
     * @param groundtruthFile
     */
    public HashMap<Integer, SuspiciousDocument> loadFile(Datafile groundtruthFile) {
        HashMap<Integer, SuspiciousDocument> map = new HashMap();
        SimilarityFile similarityFile = new SimilarityFile(groundtruthFile);
        for (SimilarityWritable similarity : similarityFile) {
            SuspiciousDocument gt = map.get(similarity.id);
            if (gt == null) {
                gt = new SuspiciousDocument(similarity);
                map.put(similarity.id, gt);
            }
            gt.add(similarity);
        }
        return map;
    }

    /**
     * 
     * @param suspiciousDoucment
     * @param score
     * @return a relevance grade for the document in the ground truth set, which 
     * for some metrics may be always 1 to avoid using relevance grades, or
     * for other metrics a descending grade over the rank to award the retrieval
     * of the most nearest neighbors.
     */
    protected abstract int getNextRelevanceGrade(SuspiciousDocument suspiciousDoucment,
            double score);

    protected class SuspiciousDocument {

        public int docid;
        public HashMap<Integer, SourceDocument> relevantDocuments = new HashMap();

        public SuspiciousDocument(SimilarityWritable similarity) {
            this.docid = similarity.id;
        }

        /**
         * Add a nearest neighbor from the ground truth, unless already k
         * nearest neighbors were registered for this document.
         *
         * @param similarity
         */
        public void add(SimilarityWritable similarity) {
            int relevanceGrade = getNextRelevanceGrade(this, similarity.score);
            if (relevanceGrade > 0) {
                SourceDocument source = 
                        new SourceDocument(similarity, 
                                relevanceGrade, 
                                relevantDocuments.size());
                relevantDocuments.put(source.docid, source);
            }
        }
    }

    protected class SourceDocument {

        public int docid;
        public int relevanceGrade;
        public int position;
        public double score;

        public SourceDocument(
                SimilarityWritable similarity, 
                int relevanceGrade, 
                int position) {
            this.docid = similarity.source;
            this.relevanceGrade = relevanceGrade;
            this.position = position;
            this.score = similarity.score;
        }
    }

}
