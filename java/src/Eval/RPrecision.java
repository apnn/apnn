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
public class RPrecision extends Metric {

    public static Log log = new Log(RPrecision.class);

    public RPrecision(ResultSet groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(SuspiciousDocument groundtruth, SuspiciousDocument retrievedDocument, int k) {
        int retrievedRelevant = 0;
        int numberRelevant = groundtruth.relevantDocuments.size();
        for (SourceDocument d : retrievedDocument.relevantDocuments.values()) {
            if (d.position <= numberRelevant) {
                SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.docid);
                if (groundTruthResult != null) {
                    retrievedRelevant++;
                }
            }
        }
        return retrievedRelevant / (double) numberRelevant;
    }
}