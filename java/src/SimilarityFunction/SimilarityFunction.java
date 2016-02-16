package SimilarityFunction;

import TestGeneric.Document;
import VocabularyPAN.Idf;
import io.github.htools.io.Datafile;

/**
 *
 * @author Jeroen
 */
public abstract class SimilarityFunction {
    private Idf idf;
    Datafile vocabularyfile;
   
    public SimilarityFunction(Datafile vocabulary) {
        vocabularyfile = vocabulary;
    }
    
    public Idf getIdf() {
        if (idf == null) {
            idf = new Idf(vocabularyfile);
        }
        return idf;
    }
        
    public abstract double similarity(Document a, Document b);
    
    public abstract int getComparisons();
    
    public abstract long getComparisonsTime();
    
    public abstract void reweight(Document a);
}
