package SimilarityFunction;

import TestGeneric.Document;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;

/**
 *
 * @author Jeroen
 */
public class InformationGain extends SimilarityFunction {
    public static Log log = new Log(InformationGain.class);

    public InformationGain(Datafile vocabulary) {
        super(vocabulary);
    }
    
    @Override
    public double similarity(Document a, Document b) {
       return a.getEModel().ignorm(b.getEModel());
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
