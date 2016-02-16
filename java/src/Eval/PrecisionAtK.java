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
public class PrecisionAtK extends MetricAtK {

    public static Log log = new Log(PrecisionAtK.class);

    public PrecisionAtK(GTMap groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(GTQuery groundtruth, ResultQuery retrievedDocument, int k) {
        int retrievedRelevant = 0;
        for (int position = 0; position < retrievedDocument.retrievedDocuments.size() && position < k; position++) {
                SourceDocument d = retrievedDocument.retrievedDocuments.get(position);
                SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.queryid);
                if (groundTruthResult != null) {
                    retrievedRelevant++;
                }
        }
        return retrievedRelevant / (double) k;
    }
}
