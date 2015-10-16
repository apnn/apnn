package Eval;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.io.Datafile;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

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
    private FHashSet<SuspiciousDocument> groundTruth;

    public Metric(Datafile groundtruthFile, int k) {
        this.k = k;
        groundTruth = loadFile(groundtruthFile);
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
        SuspiciousDocument gt = groundTruth.get(retrievedDocument);
        if (gt != null) {
            return score(gt, retrievedDocument);
        } else {
            return 0;
        }
    }

    public Set<SuspiciousDocument> getGroundTruth() {
        return Collections.unmodifiableSet(groundTruth);
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
    public HashMap<Document, Double> score(Datafile resultFile) {
        HashMap<Document, Double> results = new HashMap();
        FHashSet<SuspiciousDocument> retrievedDocuments = loadFile(resultFile);
        for (SuspiciousDocument scored : retrievedDocuments) {
            double score = score(scored);
            results.put(scored, score);
        }
        return results;
    }

    /**
     * @param scores map of scored documents (id, score)
     * @return the mean metric over the scored documents
     */
    public double mean(HashMap<Document, Double> scores) {
        return DoubleTools.mean(scores.values());
    }

    /**
     * @param file a SimilarityFile (e.g. ground truth or a results file)
     * @return a Set of the SuspiciousDocuments in the SimilarityFile with
     * the list of nearest neighbor SourceDocuments.
     */
    public FHashSet<SuspiciousDocument> loadFile(Datafile file) {
        FHashSet<SuspiciousDocument> map = new FHashSet(11100); // prevent rehashing
        SimilarityFile similarityFile = new SimilarityFile(file);
        for (SimilarityWritable similarity : similarityFile) {
            SuspiciousDocument gt = map.get(similarity.id);
            if (gt == null) {
                gt = new SuspiciousDocument(similarity);
                map.add(gt);
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

    public class Document {

        public int docid;
        
        @Override
        public int hashCode() {
            return MathTools.hashCode(docid);
        }
        
        @Override
        public boolean equals(Object o) {            
            return (o instanceof Document) && ((Document)o).docid == docid;
        }
        
        public String toString() {
            return "Doc " + docid;
        }
    }
    
    public class SuspiciousDocument extends Document {

        public FHashSet<SourceDocument> relevantDocuments = new FHashSet();

        public SuspiciousDocument(SimilarityWritable similarity) {
            this.docid = similarity.id;
        }

        public SourceDocument getSourceDocument(int sourceDocumentId) {
            return relevantDocuments.get(sourceDocumentId);
        }
        
        public SourceDocument getSourceDocument(SourceDocument sourceDocument) {
            return relevantDocuments.get(sourceDocument.docid);
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
                relevantDocuments.add(source);
            }
        }
    }

    public class SourceDocument extends Document {

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
