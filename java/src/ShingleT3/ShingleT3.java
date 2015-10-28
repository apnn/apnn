package ShingleT3;

import MinHash.*;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Document;
import TestGeneric.Tokenizer;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class ShingleT3 extends AnnIndex<FHashSetInt> {

    public static Log log = new Log(ShingleT3.class);
    int shingleSize;
    FHashMapIntList<Document> shinglesHashCodes;

    public ShingleT3(SimilarityFunction similarityFunction, int shingleSize) throws ClassNotFoundException {
        super(similarityFunction);
        initialize(shingleSize);
    }

    public ShingleT3(SimilarityFunction function, Configuration conf) throws ClassNotFoundException {
        this(function, ShingleT3Job.getShingleSize(conf));
    }

    private void initialize(int shingleSize) {
        this.shingleSize = shingleSize;
        shinglesHashCodes = new FHashMapIntList();
    }

    @Override
    protected void addDocument(Document document, FHashSetInt minHash) {
        for (int hashCode : minHash) {
            shinglesHashCodes.add(hashCode, document);
        }
    }

    @Override
    protected HashSet<Document> getDocuments(FHashSetInt shingleHashCodes, Document document) {
        HashSet<Document> documents = new HashSet();
        for (int hashCode : shingleHashCodes) {
            ObjectArrayList<Document> hashDocuments = this.shinglesHashCodes.get(hashCode);
            if (hashDocuments != null) {
                documents.addAll(hashDocuments);
            }
        }
        return documents;
    }

    @Override
    protected FHashSetInt getFingerprint(Document document) {
        FHashSetInt results = new FHashSetInt();
        ArrayList<String> terms = document.getTerms();
        for (int i = shingleSize; i <= terms.size(); i++) {
            if (Document.tokenizer.isStopword(terms.get(i - shingleSize))) {
                int hashCode = 31;
                for (int j = i - shingleSize; j < i; j++) {
                    hashCode = MathTools.combineHash(hashCode, terms.get(j).hashCode());
                }
                hashCode = MathTools.finishHash(hashCode);
                results.add(hashCode);
            }
        }
        return results;
    }
}
