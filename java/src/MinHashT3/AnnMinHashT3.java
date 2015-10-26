package MinHashT3;

import MinHash.*;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Document;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.lib.Log;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;

/**
 * Example of how you would compute 
 * @author iloen
 */
public class AnnMinHashT3 extends AnnIndex<int[]> {

    public static Log log = new Log(AnnMinHashT3.class);
    MinHashT3 minhash;
    FHashMapIntList<Document> minHashTables[];
    
    public AnnMinHashT3(SimilarityFunction similarityFunction, int numHashFunctions, int shingleSize) throws ClassNotFoundException {
        super(similarityFunction);
        initialize( numHashFunctions, shingleSize );
    }
    
    public AnnMinHashT3(SimilarityFunction function, Configuration conf) throws ClassNotFoundException {
        this(function, MinHashT3Job.getNumHashFunctions(conf), MinHashT3Job.getShingleSize(conf));
    }
    
    private void initialize(int numHashFunctions, int shingleSize) {
        minhash = new MinHashT3(numHashFunctions, shingleSize);
        minHashTables = new FHashMapIntList[numHashFunctions];
        for (int i = 0; i < numHashFunctions; i++)
            minHashTables[i] = new FHashMapIntList(10000);
    }
    
    @Override
    protected void addDocument(Document document, int[] minHash) {
        for (int i = 0; i < minHash.length; i++) {
            minHashTables[i].add(minHash[i], document);
        }
    }

    @Override
    protected HashSet<Document> getDocuments(int[] minHash, Document document) {
       HashSet<Document> documents = new HashSet();
       for (int i = 0; i < minHash.length; i++) {
           FHashMapIntList<Document> minHashtable = minHashTables[i];
           ObjectArrayList<Document> list = minHashtable.get(minHash[i]);
           if (list != null)
               documents.addAll(list);
       }
       return documents;
    }

    @Override
    protected int[] getFingerprint(Document document) {
        return minhash.getMinHash(document);
    }
}
