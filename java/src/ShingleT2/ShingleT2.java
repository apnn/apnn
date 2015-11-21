package ShingleT2;

import Shingle.AnnShingle;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Document;
import TestGeneric.Tokenizer;
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
public class ShingleT2 extends AnnShingle {

    public static Log log = new Log(ShingleT2.class);
    Tokenizer tokenizer = new Tokenizer();
    int shingleSize = 2;
    FHashMapIntList<Document> shinglesHashCodes;

    public ShingleT2(
            Comparator<SimilarityWritable> comparator,
            int shingleSize) throws ClassNotFoundException {
        super(comparator, shingleSize);
    }

    public ShingleT2(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        super(comparator, conf);
        shingleSize = conf.getInt("shinglesize", 2);
    }

    @Override
    protected FHashSetInt getFingerprint(Document document) {
        FHashSetInt results = new FHashSetInt();
        ArrayList<String> terms = document.getTerms();
        for (int ss = shingleSize; ss >= 2; ss--) {
            LOOP:
            for (int i = ss; i <= terms.size(); i++) {
                boolean doorgaan = false;
                for (int j = i - ss; j < i; j++) {
                    if (!tokenizer.isStopword(terms.get(j))) {
                        doorgaan = true;
                    }
                }
                if (doorgaan) {
                int hashCode = 31;
                for (int j = i - ss; j < i; j++) {
                    hashCode = MathTools.combineHash(hashCode, terms.get(j).hashCode());
                }
                hashCode = MathTools.finishHash(hashCode);
                results.add(hashCode);
                }
            }
        }
        return results;
    }
}
