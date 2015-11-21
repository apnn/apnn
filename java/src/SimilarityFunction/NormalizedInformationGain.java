package SimilarityFunction;

import TestGeneric.Document;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import io.github.htools.type.TermVectorEntropy;

/**
 *
 * @author Jeroen
 */
public class NormalizedInformationGain extends SimilarityFunction {
    public static Log log = new Log(NormalizedInformationGain.class);
    public static Profiler profiler = Profiler.getProfiler(NormalizedInformationGain.class.getCanonicalName());

    public NormalizedInformationGain(Datafile vocabularyFile) {
       super(vocabularyFile);
    }
    
    @Override
    public double similarity(Document a, Document b) {
       profiler.startTime();
       double result = 1 - ((TermVectorEntropy)a.getModel()).ignorm((TermVectorEntropy)b.getModel());
       profiler.addAvgTime();
       return result;
    }

    @Override
    public int getComparisons() {
       return profiler.getCount();
    }
    
    @Override
    public void reweight(Document a) {}

    @Override
    public long getComparisonsTime() {
        return profiler.getTotalTimeMs();
    }

}
