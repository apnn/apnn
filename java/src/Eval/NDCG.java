package Eval;

import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import java.io.IOException;
import java.util.HashMap;

/**
 * Evaluates the retrieved approximate nearest neighbors using NDCG vs the
 * actual top-k most similar documents. To penalize missing the nearest document
 * more than the next-to-nearest, the actual top-k nearest documents receive
 * relevance grades (k+1-position).
 *
 * @author Jeroen
 */
public class NDCG extends MetricAtK {

    public static Log log = new Log(NDCG.class);

    public NDCG(GTMap groundtruth) {
        super(groundtruth);
    }

    @Override
    public double score(GTQuery groundtruth, ResultQuery retrievedDocument, int k) {
        return dcg(groundtruth, retrievedDocument, k) / dcg(groundtruth, groundtruth, k);
    }

    /**
     * @param optimalResult
     * @param retrievedResult
     * @return the Discounted Cumulative Gain for the retrieved NNs vs the
     * optimal NNs (ground truth) for the given suspicious document.
     */
    private double dcg(GTQuery optimalResult, ResultQuery retrievedResult, int k) {
        double dcg = 0;
        for (int position = 0; position < k && position < retrievedResult.size(); position++) {
            SourceDocument document = retrievedResult.retrievedDocuments.get(position);
            // positions count from 0
            SourceDocument gt = optimalResult.relevantDocuments.get(document.queryid);
            if (gt != null) {
                if (document.position == 1) {
                    dcg += relevanceGrade(document.position, k);
                } else {
                    dcg += relevanceGrade(document.position, k) / MathTools.log2(document.position);
                }
            }
        }
        return dcg;
    }

    protected int relevanceGrade(int position, int k) {
        return 1 + k - position;
    }
}
