package SimilarityFunction;

import TestGeneric.Document;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorEntropy;

/**
 *
 * @author Jeroen
 */
public class NormalizedInformationGain implements SimilarityFunction {
    public static Log log = new Log(NormalizedInformationGain.class);
    int count = 0;

    @Override
    public double similarity(Document a, Document b) {
       count++;
       return 1 - ((TermVectorEntropy)a.getModel()).ignorm((TermVectorEntropy)b.getModel());
    }

    @Override
    public int getComparisons() {
       return count;
    }

}
