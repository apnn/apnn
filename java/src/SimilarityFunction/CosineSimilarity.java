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
       return a.getModel().cossim(b.getModel());
    }

    @Override
    public int getComparisons() {
       return count;
    }

}
