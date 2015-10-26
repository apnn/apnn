package MinHashT3;

import MinHash.*;
import TestGeneric.Document;
import TestGeneric.Tokenizer;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import io.github.htools.lib.RandomTools;
import io.github.htools.lib.RandomTools.RandomGenerator;
import java.util.ArrayList;

/**
 *
 * @author Jeroen
 */
public class MinHashT3 {

    public static Log log = new Log(MinHashT3.class);
    // use a seeded random generator, to regenerate the same has functions
    public RandomGenerator random = RandomTools.createGenerator(1);

    int[] hash;
    int shingleSize;

    public MinHashT3(int numHash, int shingleSize) {
        hash = new int[numHash];
        for (int i = 1; i < numHash; i++) {
            hash[i] = random.getInt();
        }
        this.shingleSize = shingleSize;
    }

    /**
     * @return number of (psuedo-)hash functions used.
     */
    public int hashSize() {
        return hash.length;
    }

    /**
     * @param document
     * @return a table of the minimal hashcodes over the unigrams in the
     * document using the fixed array of (psuedo-)hash functions.
     */
    public int[] getMinHash(Document document) {
        // result[i] will contain the lowest hashCode over all terms in de document
        // using function hash[i]
        int[] result = new int[hashSize()];
        ArrayTools.fill(result, Integer.MAX_VALUE);
        ArrayList<String> terms = document.getTerms();
        for (int s = shingleSize; s <= terms.size(); s++) {
            String leadTerm = terms.get(s - shingleSize);
            
            // according to specs, only shingles that start with a stop word
            if (Tokenizer.isStopword(leadTerm)) {
                int hashcode = 31;
                for (int i = s - shingleSize; i < s; i++) {
                    hashcode = MathTools.combineHash(hashcode, terms.get(i).hashCode());
                }

                // iterate over the remaining (pseudo-)hash functions
                result[0] = Math.min(result[0], hashcode);
                for (int i = 1; i < hash.length; i++) {
                    result[i] = Math.min(result[i], hashcode ^ hash[i]);
                }
            }
        }
        return result;
    }
}
