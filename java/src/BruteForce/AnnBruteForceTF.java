package BruteForce;

import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.ArrayMap;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorInt;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnBruteForceTF extends AnnIndex<TermVectorInt> {

    public static Log log = new Log(AnnBruteForceTF.class);
    ArrayMap<Document, TermVectorInt> sourceDocuments = new ArrayMap();
    
    public AnnBruteForceTF(Comparator<SimilarityWritable> comparator) throws ClassNotFoundException {
        super(comparator);
    }
    
    public AnnBruteForceTF(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(comparator);
    }
    
    @Override
    protected void addDocument(Document document, TermVectorInt vector) {
        sourceDocuments.add(document, vector);
    }

    @Override
    protected void getDocuments(CandidateList list, TermVectorInt vector, Document document) {
       for (Map.Entry<Document, TermVectorInt> entry : sourceDocuments) {
           double similarity = entry.getValue().cossim(vector);
           if (similarity < 0 || similarity > 1) {
               entry.getValue().cossimDebug(vector);
               log.fatal("getDocuments");
           }
           list.add(entry.getKey(), similarity);
       }
    }
    
    @Override
    protected TermVectorInt getFingerprint(Document document) {
        return new TermVectorInt(document.getTermsNoStopwords());
    }
}
