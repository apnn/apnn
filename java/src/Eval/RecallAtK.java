package Eval;

import io.github.htools.lib.Log;

/**
 * Computes the recall, or in this case in the absence of binary labels the
 * fraction of items in the top-k of the ground truth that are retrieved in the
 * top-k using the approach.
 *
 * @author Jeroen
 */
public class RecallAtK extends MetricAtK {

    public static Log log = new Log(RecallAtK.class);

    public RecallAtK(GTMap groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(GTQuery groundtruth, ResultQuery retrievedDocument, int k) {
        int countRetrievedTopK = 0;
        for (int position = 0; position < retrievedDocument.retrievedDocuments.size() && position < k; position++) {
                SourceDocument d = retrievedDocument.retrievedDocuments.get(position);
                SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.queryid);
                if (groundTruthResult != null) {
                    //log.info("query %s rank %d doc %s", retrievedDocument.queryid, d.position, d.queryid);
                    countRetrievedTopK++;
                }
        }
        return countRetrievedTopK / (double) groundtruth.relevantDocuments.size();
    }
}
