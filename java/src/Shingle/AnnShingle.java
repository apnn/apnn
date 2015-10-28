package Shingle;

import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Document;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnShingle extends AnnIndex<FHashSetInt> {

    public static Log log = new Log(AnnShingle.class);
    protected int shingleSize;
    protected FHashMapIntList<Document> mapShingles;
    
    public AnnShingle(SimilarityFunction similarityFunction, 
                      int shingleSize) throws ClassNotFoundException {
        super(similarityFunction);
        initialize(shingleSize );
    }
    
    public AnnShingle(SimilarityFunction function, Configuration conf) throws ClassNotFoundException {
        this(function, ShingleJob.getShingleSize(conf));
    }
    
    private void initialize(int shingleSize) {
        this.shingleSize = shingleSize;
        mapShingles = new FHashMapIntList(1000000);
        // set initial size to prevent rehashing too often
    }
    
    @Override
    protected void addDocument(Document document, FHashSetInt shingleHashCodes) {
        for (int hashCode : shingleHashCodes)
            mapShingles.add(hashCode, document);
    }
    
    @Override
    protected HashSet<Document> getDocuments(FHashSetInt shingleHashCodes, Document document) {
       HashSet<Document> documents = new HashSet();
       for (int shingle : shingleHashCodes) {
           ObjectArrayList<Document> list = mapShingles.get(shingle);
           if (list != null)
               documents.addAll(list);
       }
       return documents;
    }

    @Override
    protected FHashSetInt getFingerprint(Document document) {
        FHashSetInt result = new FHashSetInt();
        byte[] content = ByteTools.toFullTrimmed(document.getContent(), 0, document.getContent().length);
        if (content.length < shingleSize) {
            int hashcode = MathTools.hashCode(content, 0, content.length);
            result.add(hashcode);
        } else {
            for (int position = shingleSize; position < content.length; position++) {
                // if the current position is no space
                // compute the first hash function for the term
                if (content[position - shingleSize] > 32) {
                    int hashcode = MathTools.hashCode(content, position - shingleSize, position);
                    result.add(hashcode);
                }
            }
        }
        return result;
    }
}
