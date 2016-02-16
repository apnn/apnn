package ShingleWordStart;

import Shingle.AnnShingle;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Document;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import org.apache.hadoop.conf.Configuration;

import java.util.Comparator;

/**
 * @author Jeroen
 */
public class AnnShingleWordStart extends AnnShingle {

    public static Log log = new Log(AnnShingleWordStart.class);
    
    public AnnShingleWordStart(
                      Comparator<SimilarityWritable> comparator,
                      int shingleSize) throws ClassNotFoundException {
        super(comparator, shingleSize);
    }

    public AnnShingleWordStart(Comparator<SimilarityWritable> comparator,Configuration conf) throws ClassNotFoundException {
        super(comparator, conf);
    }
    
    @Override
    protected FHashSetInt getFingerprintSource(Document document) {
        FHashSetInt result = new FHashSetInt();
        byte[] content = ByteTools.toFullTrimmed(document.getContent(), 0, document.getContent().length);
        if (content.length < shingleSize) {
            int hashcode = MathTools.hashCode(content, 0, content.length);
            result.add(hashcode);
        } else {
            if (content[0] > 32) {
                int hashcode = MathTools.hashCode(content, 0, shingleSize);
                result.add(hashcode);
//                log.info("%d %d %s", document.docid, hashcode, ByteTools.toString(content, 0, shingleSize));
            }
            for (int position = shingleSize + 1; position < content.length; position++) {
                // if this is the start of a word
                // compute the first hash function for the term
                if (content[position - shingleSize - 1] == 32 && content[position - shingleSize] > 32) {
                    int hashcode = MathTools.hashCode(content, position - shingleSize, position);
                    result.add(hashcode);
//                    log.info("%d %d %s", document.docid, hashcode, ByteTools.toString(content, position - shingleSize, position));
                }
            }
        }
        //log.info("fingerprint %d %s", document.docid, new TopK<Integer>(100, result));
        return result;
    }
}
