package SimilarityFunction;

import TestGeneric.Document;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import io.github.htools.type.TermVectorDouble;

/**
 *
 * @author Jeroen
 */
public class DotProduct extends SimilarityFunction {
    public static Log log = new Log(DotProduct.class);

    public DotProduct(Datafile vocabulary) {
        super(vocabulary);
    }
    
    @Override
    public double similarity(Document a, Document b) {
       return a.getModel().dotproduct(b.getModel());
    }

    @Override
    public int getComparisons() {
       return 0;
    }

    @Override
    public long getComparisonsTime() {
       return 0;
    }

    public void reweight(Document a) {

    }
}
