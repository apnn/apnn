package Eval;

import io.github.htools.lib.Log;

/**
 * Computes the K-Recall over the exact cosine similarity, which is the Recall@K 
 * considering only K relevant items, therefore retrieving the maximum number 
 * of available relevant documents at rank K will consistently return 1.
 *
 * @author Jeroen
 */
public class KRecall extends Metric {

    public static Log log = new Log(KRecall.class);

    public KRecall(ResultSet groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(SuspiciousDocument groundtruth, SuspiciousDocument retrievedDocument, int k) {
        int countRetrievedTopK = 0;
        for (SourceDocument d : retrievedDocument.relevantDocuments.values()) {
            if (d.position <= k) {
                SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.docid);
                if (groundTruthResult != null) {
                    countRetrievedTopK++;
                }
            }
        }
        return countRetrievedTopK / (double) Math.min(k, groundtruth.relevantDocuments.size());
    }
}
