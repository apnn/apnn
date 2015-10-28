package ShingleWordStart;

import Shingle.*;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.Document;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnShingleWordStart extends AnnShingle {

    public static Log log = new Log(AnnShingleWordStart.class);
    
    public AnnShingleWordStart(SimilarityFunction similarityFunction, 
                      int shingleSize) throws ClassNotFoundException {
        super(similarityFunction, shingleSize);
    }

    public AnnShingleWordStart(SimilarityFunction function, Configuration conf) throws ClassNotFoundException {
        super(function, conf);
    }
    
    @Override
    protected FHashSetInt getFingerprint(Document document) {
        FHashSetInt result = new FHashSetInt();
        byte[] content = ByteTools.toFullTrimmed(document.getContent(), 0, document.getContent().length);
        if (content.length < shingleSize) {
            int hashcode = MathTools.hashCode(content, 0, content.length);
            result.add(hashcode);
        } else {
            if (content[0] > 32) {
                int hashcode = MathTools.hashCode(content, 0, shingleSize);
                result.add(hashcode);
            }
            for (int position = shingleSize + 1; position < content.length; position++) {
                // if the current position is no space
                // compute the first hash function for the term
                if (content[position - shingleSize - 1] == 32 && content[position - shingleSize] > 32) {
                    int hashcode = MathTools.hashCode(content, position - shingleSize, position);
                    result.add(hashcode);
                }
            }
        }
        //log.info("fingerprint %d %s", document.docid, new TopK<Integer>(100, result));
        return result;
    }
}
