package MinHash;

import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Document;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.lib.Log;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnMinHash extends AnnIndex<int[]> {

    public static Log log = new Log(AnnMinHash.class);
    MinHash minhash;
    FHashMapIntList<Document> minHashTables[];
    
    public AnnMinHash(SimilarityFunction similarityFunction, int numHashFunctions, int bandwidth) throws ClassNotFoundException {
        super(similarityFunction);
        log.info("AnnMinhash consructor");
        initialize( numHashFunctions, bandwidth );
    }
    
    public AnnMinHash(SimilarityFunction function, Configuration conf) throws ClassNotFoundException {
        this(function, MinHashJob.getNumHashFunctions(conf), MinHashJob.getBandwidth(conf));
    }
    
    private void initialize(int numHashFunctions, int bandwidth) {
        minhash = new MinHash(numHashFunctions, bandwidth);
        minHashTables = new FHashMapIntList[minhash.getBandCount()];
        for (int i = 0; i < minhash.getBandCount(); i++)
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
        int[] fingerprint = minhash.getMinHash(document);
        return fingerprint;
    }
}
