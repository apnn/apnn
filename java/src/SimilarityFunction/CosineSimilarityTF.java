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
public class CosineSimilarityTF extends SimilarityFunction {
    public static Log log = new Log(CosineSimilarityTF.class);
    public static Profiler profiler = Profiler.getProfiler(CosineSimilarityTF.class.getCanonicalName());

    public CosineSimilarityTF(Datafile vocabularyfile) {
        super(vocabularyfile);
    }
    
    @Override
    public double similarity(Document a, Document b) {
        profiler.startTime();
        double cossim = a.getModel().cossim(b.getModel());
        profiler.addAvgTime();
        if (Double.isNaN(cossim) || cossim > 1) {
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

    public void reweight(Document a) {
    }

    @Override
    public long getComparisonsTime() {
        return profiler.getTotalTimeMs();
    }
}
