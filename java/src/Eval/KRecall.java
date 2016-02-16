package Eval;

import io.github.htools.lib.Log;

/**
 * Computes the K-Recall over the exact cosine similarity, which is the Recall@K
 * considering only K relevant items, therefore retrieving the maximum number of
 * available relevant documents at rank K will consistently return 1.
 *
 * @author Jeroen
 */
public class KRecall extends MetricAtK {

    public static Log log = new Log(KRecall.class);

    public KRecall(GTMap groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(GTQuery groundtruth, ResultQuery retrievedDocument, int k) {
        int countRetrievedTopK = 0;
        for (int position = 0; position < k && position < retrievedDocument.size(); position++) {
            SourceDocument d = retrievedDocument.retrievedDocuments.get(position);
            SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.queryid);
            if (groundTruthResult != null) {
                countRetrievedTopK++;
            }
        }
        return countRetrievedTopK / (double) Math.min(k, groundtruth.relevantDocuments.size());
    }
}
