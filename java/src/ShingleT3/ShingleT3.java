package ShingleT3;

import Shingle.AnnShingle;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Document;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class ShingleT3 extends AnnShingle {

    public static Log log = new Log(ShingleT3.class);
    int shingleSize = 3;
    FHashMapIntList<Document> shinglesHashCodes;

    public ShingleT3(
                      Comparator<SimilarityWritable> comparator,
                      int shingleSize) throws ClassNotFoundException {
        super(comparator, shingleSize);
    }

    public ShingleT3(Comparator<SimilarityWritable> comparator,Configuration conf) throws ClassNotFoundException {
        super(comparator, conf);
        shingleSize = conf.getInt("shinglesize", 3);
    }

    @Override
    protected FHashSetInt getFingerprint(Document document) {
        FHashSetInt results = new FHashSetInt();
        ArrayList<String> terms = document.getTerms();
        for (int i = shingleSize; i <= terms.size(); i++) {
            //if (Document.tokenizer.isStopword(terms.get(i - shingleSize))) {
                int hashCode = 31;
                for (int j = i - shingleSize; j < i; j++) {
                    hashCode = MathTools.combineHash(hashCode, terms.get(j).hashCode());
                }
                hashCode = MathTools.finishHash(hashCode);
                results.add(hashCode);
            //}
        }
        return results;
    }
}
