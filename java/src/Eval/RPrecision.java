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
public class RPrecision extends MetricNoK {

    public static Log log = new Log(RPrecision.class);

    public RPrecision(GTMap groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(GTQuery groundtruth, ResultQuery retrievedDocument) {
        int retrievedRelevant = 0;
        int numberRelevant = groundtruth.relevantDocuments.size();
        for (int position = 0; position < numberRelevant && position < retrievedDocument.size(); position++) {
            SourceDocument d = retrievedDocument.retrievedDocuments.get(position);
            SourceDocument groundTruthResult = groundtruth.getSourceDocument(d.queryid);
            if (groundTruthResult != null) {
                retrievedRelevant++;
            }
        }
        return retrievedRelevant / (double) numberRelevant;
    }
}
