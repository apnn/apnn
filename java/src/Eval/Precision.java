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
public class Precision extends MetricNoK {

    public static Log log = new Log(Precision.class);

    public Precision(GTMap groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(GTQuery groundtruth, ResultQuery retrievedDocument) {
        int retrievedRelevant = 0;
        int retrievedIrrelevant = 0;
        for (int position = 0; position < retrievedDocument.size(); position++) {
            SourceDocument d = retrievedDocument.retrievedDocuments.get(position);
            SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.queryid);
            if (groundTruthResult != null) {
                retrievedRelevant++;
            } else {
                retrievedIrrelevant++;
            }
        }
        return retrievedRelevant / (double) (retrievedRelevant + retrievedIrrelevant);
    }
}
