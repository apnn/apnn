package SimilarityFunction;

import TestGeneric.Document;
import Vocabulary.Idf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import io.github.htools.type.TermVectorDouble;
import io.github.htools.type.TermVectorInt;

/**
 *
 * @author Jeroen
 */
public class CosineSimilarityTFIDF implements SimilarityFunction {
    public static Log log = new Log(CosineSimilarityTFIDF.class);
    public static Idf idf;
    public static Profiler profiler = Profiler.getProfiler(CosineSimilarityTFIDF.class.getCanonicalName());

    public CosineSimilarityTFIDF(Datafile vocabulary) {
        idf = new Idf(vocabulary);
    }
    
    @Override
    public double similarity(Document a, Document b) {
        profiler.startTime();
        double cossim = a.getModel().cossim(b.getModel());
        profiler.addAvgTime();
        if (Double.isNaN(cossim) || cossim > 1) {
            if (a.getModel() instanceof TermVectorInt)
                ((TermVectorInt)a.getModel()).cossimDebug((TermVectorInt)b.getModel());
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
        TermVectorDouble multiply = a.getModel().multiply(idf);
        a.setModel(multiply);
    }
}
