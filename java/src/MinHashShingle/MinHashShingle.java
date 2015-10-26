package MinHashShingle;

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
public class MinHashShingle {

    public static Log log = new Log(MinHashShingle.class);
    // use a seeded random generator, to regenerate the same has functions
    public RandomGenerator random = RandomTools.createGenerator(1);

    int[] hash;
    int shinglesize;

    public MinHashShingle(int numHash, int singlesize) {
        hash = new int[numHash];
        for (int i = 1; i < numHash; i++) {
            hash[i] = random.getInt();
        }
        this.shinglesize = shinglesize;
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

        byte[] content = document.getContent();
        if (content.length < shinglesize) {
            int hashcode = MathTools.hashCode(content, 0, content.length);
            result[0] = Math.min(result[0], hashcode);
            for (int i = 1; i < hash.length; i++) {
                result[i] = Math.min(result[i], hashcode ^ hash[i]);
            }
        } else {
            for (int position = shinglesize; position < content.length; position++) {
                // the first hash function for the term
                int hashcode = MathTools.hashCode(content, position - shinglesize, position);
                result[0] = Math.min(result[0], hashcode);
                // iterate over the remaining (pseudo-)hash functions
                for (int i = 1; i < hash.length; i++) {
                    result[i] = Math.min(result[i], hashcode ^ hash[i]);
                }
            }
        }
        return result;
    }
}
