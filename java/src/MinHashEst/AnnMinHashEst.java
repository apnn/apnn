package MinHashEst;

import MinHash.AnnMinHash;
import MinHash.MinHashJob;
import SimilarityFile.SimilarityWritable;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.HashMapInt;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.lib.Log;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnMinHashEst extends AnnMinHash {

    public static Log log = new Log(AnnMinHashEst.class);

    public AnnMinHashEst(Comparator<SimilarityWritable> comparator, int numHashFunctions, int bandwidth) throws ClassNotFoundException {
        super(comparator, numHashFunctions, bandwidth);
    }

    public AnnMinHashEst(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(comparator, MinHashJob.getNumHashFunctions(conf), MinHashJob.getBandwidth(conf));
    }

    @Override
    protected void getDocuments(CandidateList candidates, int[] minHash, Document query) {
        HashMapInt<Document> documents = new HashMapInt();
        for (int i = 0; i < minHash.length; i++) {
            FHashMapIntList<Document> minHashtable = minHashTables[i];
            ObjectArrayList<Document> list = minHashtable.get(minHash[i]);
            if (list != null) {
                documents.addAll(list);
            }
        }
        for (Map.Entry<Document, Integer> entry : documents.entrySet()) {
            double indexSimilarity = entry.getValue() / (double) minHash.length;
            candidates.add(entry.getKey(), indexSimilarity);
        }
    }
}
