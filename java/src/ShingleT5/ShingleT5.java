package ShingleT5;

import Shingle.AnnShingle;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Document;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author Jeroen
 */
public class ShingleT5 extends AnnShingle {

    public static Log log = new Log(ShingleT5.class);
    int shingleSize = 5;
    FHashMapIntList<Document> shinglesHashCodes;

    public ShingleT5(
                      Comparator<SimilarityWritable> comparator,
                      int shingleSize) throws ClassNotFoundException {
        super(comparator, shingleSize);
    }

    public ShingleT5(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        super(comparator, conf);
        shingleSize = conf.getInt("shinglesize", 5);
    }

    @Override
    public FHashSetInt getFingerprintSource(Document document) {
        FHashSetInt results = new FHashSetInt();
        ArrayList<String> terms = document.getTermsStopwords();
        for (int i = shingleSize; i <= terms.size(); i++) {
            //if (!Document.getContentExtractor().isStopword(terms.get(i - shingleSize))) {
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
