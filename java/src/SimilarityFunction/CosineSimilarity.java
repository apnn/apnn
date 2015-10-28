package SimilarityFunction;

import TestGeneric.Document;
import io.github.htools.lib.Log;

/**
 *
 * @author Jeroen
 */
public class CosineSimilarity implements SimilarityFunction {
    public static Log log = new Log(CosineSimilarity.class);
    int count = 0;

    @Override
    public double similarity(Document a, Document b) {
        count++;
        double cossim = a.getModel().cossim(b.getModel());
        if (Double.isNaN(cossim) || cossim > 1) {
            a.getModel().cossimDebug(b.getModel());
            log.fatal("cossim %s", cossim);
        }
       return cossim;
    }

    @Override
    public int getComparisons() {
       return count;
    }

}
