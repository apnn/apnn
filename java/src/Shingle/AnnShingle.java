package Shingle;

import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.HashMapInt;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnShingle extends AnnIndex<FHashSetInt> {

    public static Log log = new Log(AnnShingle.class);
    protected int shingleSize;
    protected FHashMapIntList<Document> mapShingles;

    public AnnShingle(
            Comparator<SimilarityWritable> comparator,
            int shingleSize) throws ClassNotFoundException {
        super(comparator);
        initialize(shingleSize);
    }

    public AnnShingle(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(comparator, ShingleJob.getShingleSize(conf));
    }

    private void initialize(int shingleSize) {
        this.shingleSize = shingleSize;
        mapShingles = new FHashMapIntList(1000000);
        // set initial size to prevent rehashing too often
    }

    @Override
    protected void addDocument(Document document, FHashSetInt shingleHashCodes) {
        for (int hashCode : shingleHashCodes) {
            mapShingles.add(hashCode, document);
        }
    }
    
    @Override
    protected void getDocuments(CandidateList candidates, FHashSetInt shingleHashCodes, Document document) {
        HashMapInt<Document> docCount = new HashMapInt();
        log.info("fp size %s %d", document.docid, shingleHashCodes.size());
        this.countDocCodepoints += shingleHashCodes.size();
        for (int shingle : shingleHashCodes) {
            ObjectArrayList<Document> list = mapShingles.get(shingle);
            if (list != null) {
                docCount.addAll(list);
                this.countComparedDocCodepoints += list.size();
            }
        }
        for (Map.Entry<Document, Integer> entry : docCount.entrySet()) {
            double estimatedSimilarity = entry.getValue() / (double) shingleHashCodes.size();
            candidates.add(entry.getKey(), estimatedSimilarity); // todo add estimation similarity
        }
    }

    @Override
    protected FHashSetInt getFingerprint(Document document) {
        FHashSetInt result = new FHashSetInt();
        byte[] content = ByteTools.toFullTrimmed(document.getTokenizedContent(), 
                0, document.getTokenizedContent().length);
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
