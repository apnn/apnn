package MinHash;

import Canopy.AnnCanopy.Doc;
import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.lib.Log;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.hadoop.conf.Configuration;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Jeroen
 */
public class AnnMinHash extends AnnIndex<int[]> {

    public static Log log = new Log(AnnMinHash.class);
    protected MinHash minhash;
    protected FHashMapIntList<Document> minHashTables[];
    
    public AnnMinHash(Comparator<SimilarityWritable> comparator, int numHashFunctions, int bandwidth)  {
        super(comparator);
        //log.info("AnnMinhash constructor %d %d", numHashFunctions, bandwidth);
        initialize( numHashFunctions, bandwidth );
    }
    
    public AnnMinHash(Comparator<SimilarityWritable> comparator, Configuration conf) {
        this(comparator, MinHashJob.getNumHashFunctions(conf), MinHashJob.getBandwidth(conf));
    }
    
    private void initialize(int numHashFunctions, int bandwidth) {
        minhash = new MinHash(numHashFunctions, bandwidth);
        minHashTables = new FHashMapIntList[minhash.getBandCount()];
        for (int i = 0; i < minhash.getBandCount(); i++)
            minHashTables[i] = new FHashMapIntList(10000);
    }
    
    @Override
    protected void addDocument(Document document, int[] minhash) {
        Doc<int[]> doc = this.createDocument(document, minhash);
        for (int i = 0; i < minhash.length; i++) {
            minHashTables[i].add(minhash[i], doc.getDocument());
        }
    }

    @Override
    protected void getDocuments(CandidateList list, int[] minhash, Document doc) {
       HashMapDouble<Document> documentCount = new HashMapDouble();
       for (int hashFunction = 0; hashFunction < minhash.length; hashFunction++) {
           FHashMapIntList<Document> minHashtable = minHashTables[hashFunction];
           ObjectArrayList<Document> bucket = minHashtable.get(minhash[hashFunction]);
           if (bucket != null)
               documentCount.addAll(bucket);
       }
       for (Map.Entry<Document, Double> entry : documentCount.entrySet()) {
           list.add(entry.getKey(), entry.getValue());
       }
    }

    @Override
    public int[] getFingerprintSource(Document document) {
        int[] fingerprint = minhash.getMinHash(document);
        //log.info("%d %s", document.docid, ArrayTools.toString(fingerprint));
        return fingerprint;
    }
}
