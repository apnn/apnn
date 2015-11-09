package SimilarityFunction;

import TestGeneric.Document;

/**
 *
 * @author Jeroen
 */
public interface SimilarityFunction {
    public double similarity(Document a, Document b);
    
    public int getComparisons();
    
    public long getComparisonsTime();
    
    public void reweight(Document a);
}
