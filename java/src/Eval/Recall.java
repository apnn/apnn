package Eval;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;

/**
 * Computes the recall, or in this case in the absence of binary labels the
 * fraction of items in the top-k of the ground truth that are retrieved in the
 * top-k using the approach.
 *
 * @author Jeroen
 */
public class Recall extends Metric {

    public static Log log = new Log(Recall.class);

    public Recall(Datafile groundtruthFile) {
        super(groundtruthFile);
    }

    @Override
    public double score(SuspiciousDocument groundtruth, SuspiciousDocument retrievedDocument, int k) {
        int countRetrievedTopK = 0;
        for (SourceDocument d : retrievedDocument.relevantDocuments.values()) {
            if (d.position <= k) {
                SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.docid);
                if (groundTruthResult != null && groundTruthResult.position <= k) {
                    countRetrievedTopK++;
                }
            }
        }
        return countRetrievedTopK / (double) Math.min(groundtruth.relevantDocuments.size(), k);
    }
}
