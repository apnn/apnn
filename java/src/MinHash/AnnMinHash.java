package MinHash;

import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.lib.Log;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnMinHash extends AnnIndex<int[]> {

    public static Log log = new Log(AnnMinHash.class);
    protected MinHash minhash;
    protected FHashMapIntList<Document> minHashTables[];
    
    public AnnMinHash(SimilarityFunction similarityFunction, Comparator<SimilarityWritable> comparator, int numHashFunctions, int bandwidth) throws ClassNotFoundException {
        super(similarityFunction, comparator);
        log.info("AnnMinhash constructor %d %d", numHashFunctions, bandwidth);
        initialize( numHashFunctions, bandwidth );
    }
    
    public AnnMinHash(SimilarityFunction function, Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(function, comparator, MinHashJob.getNumHashFunctions(conf), MinHashJob.getBandwidth(conf));
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
    protected void getDocuments(CandidateList list, int[] minHash, Document document) {
       HashMapDouble<Document> documentCount = new HashMapDouble();
       for (int hashFunction = 0; hashFunction < minHash.length; hashFunction++) {
           FHashMapIntList<Document> minHashtable = minHashTables[hashFunction];
           ObjectArrayList<Document> bucket = minHashtable.get(minHash[hashFunction]);
           if (bucket != null)
               documentCount.addAll(bucket);
       }
       for (Map.Entry<Document, Double> entry : documentCount.entrySet()) {
           list.add(entry.getKey(), entry.getValue());
       }
    }

    @Override
    protected int[] getFingerprint(Document document) {
        int[] fingerprint = minhash.getMinHash(document);
        //log.info("%d %s", document.docid, ArrayTools.toString(fingerprint));
        return fingerprint;
    }
}
