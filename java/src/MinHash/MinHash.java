package MinHash;

import TestGeneric.Document;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import io.github.htools.lib.RandomTools;
import io.github.htools.lib.RandomTools.RandomGenerator;

/**
 * 
 * @author Jeroen
 */
public class MinHash {
    public static Log log = new Log(MinHash.class);
    // use a seeded random generator, to regenerate the same has functions
    public RandomGenerator random = RandomTools.createGenerator(1);
    
    int[] hash;
    int bands;
    int bandwidth;

    public MinHash(int numHash, int bandwidth) {
        hash = new int[numHash];
        for (int i = 1; i < numHash; i++) {
            hash[i] = random.getInt();
        }
        if (numHash % bandwidth != 0)
            log.fatal("MinHash numhash %d not a multiple of bandwidth %d", numHash, bandwidth);
        bands = numHash / bandwidth;
        this.bandwidth = bandwidth;
    }
    
    /**
     * @return number of (psuedo-)hash functions used.
     */
    public int hashSize() {
        return hash.length;
    }
    
    /**
     * @param document
     * @return a table of the minimal hashcodes over the unigrams in the document
     * using the fixed array of (psuedo-)hash functions.
     */
    private int[] getMinHashCodes(Document document) {
        // result[i] will contain the lowest hashCode over all terms in de document
        // using function hash[i]
        int[] result = new int[hashSize()];
        ArrayTools.fill(result, Integer.MAX_VALUE);
        
        for (String term : document.getModel().keySet()) {
            // the first hash function for the term
            int hashcode = MathTools.hashCode(term.hashCode());
            result[0] = Math.min(result[0], hashcode);
            
            // iterate over the remaining (pseudo-)hash functions
            for (int i = 1; i < hash.length; i++) {
                result[i] = Math.min(result[i], hashcode ^ hash[i]);
            }
        }
        log.info("getMinHash %d %s", document.getId(), ArrayTools.toString(result));
        return result;
    }
    
    public int[] getMinHash(Document document) {
        // result[i] will contain the lowest hashCode over all terms in de document
        // using function hash[i]
        int[] minhash = getMinHashCodes(document);
        int[] bands = new int[this.bands];
        for (int i = 0; i < minhash.length; i++) {
            bands[i / bandwidth] ^= minhash[i];
        }
        return bands;
    }
    
    
}
