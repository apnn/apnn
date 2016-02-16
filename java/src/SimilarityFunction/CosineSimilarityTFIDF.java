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
public class CosineSimilarityTFIDF extends SimilarityFunction {
    public static Log log = new Log(CosineSimilarityTFIDF.class);
    public static Profiler profiler = Profiler.getProfiler(CosineSimilarityTFIDF.class.getCanonicalName());

    public CosineSimilarityTFIDF(Datafile vocabulary) {
        super(vocabulary);
    }
    
    @Override
    public double similarity(Document a, Document b) {
        double cossim = a.getModel().cossim(b.getModel());
        if (Double.isNaN(cossim) || cossim > 1.000001) {
            if (a.getModel() instanceof TermVectorDouble)
                ((TermVectorDouble)a.getModel()).cossimDebug((TermVectorDouble)b.getModel());
            log.fatal("cossim %s", cossim);
        }
       return cossim;
    }

    @Override
    public int getComparisons() {
       return profiler.getCount();
    }

    @Override
    public long getComparisonsTime() {
       return profiler.getTotalTimeMs();
    }

    public void reweight(Document a) {
        TermVectorDouble multiply = a.getModel().multiply(getIdf());
        a.setModel(multiply);
    }
}
