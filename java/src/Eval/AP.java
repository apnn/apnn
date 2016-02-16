package Eval;

import io.github.htools.io.Datafile;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Computes the recall, or in this case in the absence of binary labels the
 * fraction of items in the top-k of the ground truth that are retrieved in the
 * top-k using the approach.
 *
 * @author Jeroen
 */
public class AP extends MetricNoK {

    public static Log log = new Log(AP.class);
    Precision precision;
    Recall recall;

    public AP(GTMap groundtruth) {
        super(groundtruth);
        precision = new Precision(groundtruth);
        recall = new Recall(groundtruth);
    }

    /**
     * @param resultFile
     * @return a map of scored documents (id, score) for the retrieved nearest
     * neighbors in the given file.
     */
    public double score(GTQuery query, ResultQuery retrievedQuery) {
        int totalRelevant = query.relevantDocuments.size();
        if (totalRelevant > 0) {
            int relevant = 0;
            double prec = 0;
            double recall = 0;
            double[] precInterval = new double[11];
            for (int i = 0; i < retrievedQuery.size(); i++) {
                SourceDocument d = retrievedQuery.retrievedDocuments.get(i);
                SourceDocument r = query.getSourceDocument(d.queryid);
                if (r != null) {
                    relevant++;
                    prec = relevant / (double) (i + 1);
                    recall = relevant / (double) totalRelevant;
                    int pos = (int) Math.floor(recall * 10);
                    for (; pos >= 0 && precInterval[pos] < prec; pos--) {
                        precInterval[pos] = prec;
                    }
                }
            }
            return DoubleTools.mean(precInterval);
        }
        return 0;
    }
}
