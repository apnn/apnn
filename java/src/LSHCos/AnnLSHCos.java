package LSHCos;

import static LSHCos.AnnLSHCos.COUNTERS.LSHABSERROR;
import static LSHCos.AnnLSHCos.COUNTERS.LSHERRORCOUNT;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarityTFIDF;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import Vocabulary.Idf;
import io.github.htools.collection.ArrayMap;
import io.github.htools.fcollection.FHashMap;
import io.github.htools.lib.Log;
import io.github.htools.lib.RandomTools;
import io.github.htools.type.TermVectorDouble;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author Jeroen
 */
public class AnnLSHCos extends AnnIndex<long[]> {

    public static Log log = new Log(AnnLSHCos.class);
    enum COUNTERS {
        LSHABSERROR,
        LSHERRORCOUNT
    }
    protected int numHyperplanes = 100;
    protected int fingerprintSize = numHyperplanes / 64;
    protected Idf idf;
    FHashMap<String, float[]> randomPlane;
    ArrayMap<Document, long[]> index = new ArrayMap();
    double abserror;
    int errorcount;

    public AnnLSHCos(SimilarityFunction similarityFunction,
            Comparator<SimilarityWritable> comparator,
            int numHyperplanes) throws ClassNotFoundException {
        super(similarityFunction, comparator);
        initialize(numHyperplanes);
    }

    public AnnLSHCos(SimilarityFunction function, Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(function, comparator, LSHCosJob.getNumHyperplanes(conf));
    }

    private void initialize(int numVectors) {
        idf = ((CosineSimilarityTFIDF) this.similarityFunction).idf;
        RandomTools.RandomGenerator random = RandomTools.createGenerator(0);
        this.numHyperplanes = numVectors;
        fingerprintSize = 1 + (numVectors - 1) / 64;
        randomPlane = new FHashMap(idf.size());
        for (String term : idf.keySet()) {
            float[] weights = new float[numVectors];
            for (int i = 0; i < numVectors; i++) {
                weights[i] = (float)random.getStdNormal();
            }
            randomPlane.put(term, weights);
        }
    }

    @Override
    protected void addDocument(Document document, long[] fp) {
        index.add(document, fp);
    }

    @Override
    protected void getDocuments(CandidateList candidates, long[] fp, Document document) {
        //log.info("fp size %d %s", document.docid, Long.toBinaryString(fp[0]));
        //log.info("%d %s", document.docid, fp);
        for (Map.Entry<Document, long[]> entry : index) {
            long[] entryFp = entry.getValue();
            int dissim = 0;
            for (int i = 0; i < fingerprintSize; i++) {
                dissim += Long.bitCount(fp[i] ^ entryFp[i]);
            }
            //if (document.docid == 1) {
                //long a = fp[0] ^ entryFp[0];
                //long b = fp[1] ^ entryFp[1];
//                log.printf("%5d %64s\n%5d %64s\n%5d %64s", document.docid, Long.toBinaryString(fp[0]), 
//                                                         entry.getKey().docid, Long.toBinaryString(entryFp[0]), 
//                                                         dissim, Long.toBinaryString(a));
//                log.printf("%5d %64s\n%5d %64s\n%5d %64s\n", document.docid, Long.toBinaryString(fp[1]), 
//                                                         entry.getKey().docid, Long.toBinaryString(entryFp[1]), 
//                                                         dissim, Long.toBinaryString(b));
            //}
            double similarity = Math.cos((1 - (numHyperplanes - dissim) / (double) numHyperplanes) * Math.PI);
            candidates.add(entry.getKey(), similarity);
        }
    }

    @Override
    protected long[] getFingerprint(Document document) {
        double[] offsetToHyperplane = new double[numHyperplanes];
        for (Object2DoubleMap.Entry<String> entry : ((TermVectorDouble) document.getModel()).object2DoubleEntrySet()) {
            float[] planeWeights = randomPlane.get(entry.getKey());
            if (planeWeights != null) {
                double tfidf = entry.getDoubleValue();
                for (int h = 0; h < numHyperplanes; h++) {
                    offsetToHyperplane[h] += planeWeights[h] * tfidf;
                }
            }
        }
        long[] fingerprint = new long[fingerprintSize];
        Arrays.fill(fingerprint, 0);
        for (int i = 0; i < numHyperplanes; i++) {
            int pos = i / 64;
            if (offsetToHyperplane[i] > 0) {
                fingerprint[pos] |= (1l << (i % 64));
            }
        }
        return fingerprint;
    }
    
    @Override
    protected void assignMeasureSimilarity(CandidateList candidates, Document document) {
        super.assignMeasureSimilarity(candidates, document);
        for (Candidate c : candidates) {
            abserror += Math.abs(c.indexSimilarity - c.measureSimilarity);
            errorcount++;
        }
    }
    
    @Override
    public void cleanup(Context context) {
        context.getCounter(LSHABSERROR).increment((long)(abserror * 1000000));
        context.getCounter(LSHERRORCOUNT).increment(errorcount);
    }
}
