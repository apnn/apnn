package LSHCosOld;

import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.ArrayMap;
import io.github.htools.fcollection.FHashMap;
import io.github.htools.lib.Log;
import io.github.htools.lib.RandomTools;
import io.github.htools.type.TermVectorDouble;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;

/**
 * @author Jeroen
 */
public class AnnLSHCos extends AnnIndex<long[]> {

    public static Log log = new Log(AnnLSHCos.class);
    RandomTools.RandomGenerator random = RandomTools.createGenerator(0);
    protected int numHyperplanes = 100;
    protected int fingerprintSize = numHyperplanes / 64;
    HashSet<String> vocabulary;
    FHashMap<String, float[]> randomPlane;
    ArrayMap<Document, long[]> index = new ArrayMap();
    ArrayMap<Document, long[]> queries = new ArrayMap();

    public AnnLSHCos(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        super(comparator);
        this.numHyperplanes = LSHCosJob.getNumHyperplanes(conf);
        fingerprintSize = 1 + (numHyperplanes - 1) / 64;
    }

    public void createRandomPlanes64(int part) {
        int bits = part < fingerprintSize - 1 ? 64 : numHyperplanes - 64 * (fingerprintSize - 1);
        randomPlane = new FHashMap(vocabulary.size());
        for (String term : vocabulary) {
            float[] weights = new float[bits];
            for (int i = 0; i < bits; i++) {
                weights[i] = (float) random.getStdNormal();
            }
            randomPlane.put(term, weights);
        }
    }

    public Iterable<Map.Entry<Document, long[]>> queryIterator() {
        return queries;
    }

    protected void generateHyperplanes() {
        random = RandomTools.createGenerator(0);

        vocabulary = new HashSet(10000);
        for (Document d : index.keySet()) {
            vocabulary.addAll(d.getModel().keySet());
        }
        for (Document d : queries.keySet())
            vocabulary.addAll(d.getModel().keySet());
    }

    public void set(ArrayList<Document> source, ArrayList<Document> queries) {
        index = createEmptyFingerPrints(source);
        this.queries = createEmptyFingerPrints(queries);
        generateHyperplanes();
        for (int part = 0; part < fingerprintSize; part++) {
            createRandomPlanes64(part);
            int bits = part < fingerprintSize - 1 ? 64 : numHyperplanes - 64 * (fingerprintSize - 1);
            createFingerprint(index, part, bits);
            createFingerprint(this.queries, part, bits);
        }
    }

    public void set(ArrayList<Document> source) {
        index = createEmptyFingerPrints(source);
        queries = new ArrayMap();
        generateHyperplanes();
        for (int part = 0; part < fingerprintSize; part++) {
            createRandomPlanes64(part);
            int bits = part < fingerprintSize - 1 ? 64 : numHyperplanes - 64 * (fingerprintSize - 1);
            createFingerprint(index, part, bits);
        }
    }

    public void createFingerprint(ArrayMap<Document, long[]> map, int part, int bits) {
        for (Map.Entry<Document, long[]> entry : map) {
            long fp = getFingerprint(entry.getKey(), part, bits);
            entry.getValue()[part] = fp;
        }
    }

    public ArrayMap<Document, long[]> createEmptyFingerPrints(ArrayList<Document> documents) {
        ArrayMap<Document, long[]> result = new ArrayMap();
        for (Document d : documents) {
            result.add(d, new long[fingerprintSize]);
        }
        return result;
    }

    protected void addDocument(Document document, long[] fp) {
        index.add(document, fp);
        document.clearContent();
        document.clearTerms();
    }

    protected void getDocuments(CandidateList candidates, long[] fp, Document document) {
        for (Map.Entry<Document, long[]> entry : index) {
            if (document != entry.getKey()) {
                long[] entryFp = entry.getValue();
                if (entryFp == null || fp == null) {
                    log.info("%s %s %s %s", document.getId(), fp, entry.getKey().getId(), entryFp);
                }
                int dissim = 0;
                for (int i = 0; i < fingerprintSize; i++) {
                    dissim += Long.bitCount(fp[i] ^ entryFp[i]);
                }
//                if (document.getId().equals("1")) {
//                long a = fp[0] ^ entryFp[0];
//                long b = fp[1] ^ entryFp[1];
//                    log.printf("%5s %64s\n%5s %64s\n%5d %64s", document.docid, Long.toBinaryString(fp[0]),
//                            entry.getKey().docid, Long.toBinaryString(entryFp[0]),
//                            dissim, Long.toBinaryString(a));
//                    log.printf("%5s %64s\n%5s %64s\n%5d %64s\n", document.docid, Long.toBinaryString(fp[1]),
//                            entry.getKey().docid, Long.toBinaryString(entryFp[1]),
//                            dissim, Long.toBinaryString(b));
//                }
                double similarity = Math.cos((1 - (numHyperplanes - dissim) / (double) numHyperplanes) * Math.PI);
                candidates.add(entry.getKey(), similarity);
            }
        }
        for (Candidate c : candidates) {
            c.measureSimilarity = document.similarity(c.document);
        }
    }

    public long[] getFingerprintSource(Document document) {
        long[] fp = index.get(document);

        return fp;
    }

    public long[] getFingerprintQuery(Document document) {
        long[] fp;
        if (queries.size() == 0)
            fp = index.get(document);
        else
            fp = queries.get(document);

        return fp;
    }

    protected long getFingerprint(Document document, int part, int bits) {
        //log.info("getFingerPrint %d %d", part, bits);
        double[] offsetToHyperplane = new double[bits];
        for (Object2DoubleMap.Entry<String> entry : ((TermVectorDouble) document.getModel()).object2DoubleEntrySet()) {
            float[] planeWeights = randomPlane.get(entry.getKey());
            if (planeWeights != null) {
                double tfidf = entry.getDoubleValue();
                for (int h = 0; h < bits; h++) {
                    offsetToHyperplane[h] += planeWeights[h] * tfidf;
                }
            }
        }
        long fingerprint = 0;
        for (int i = 0; i < bits; i++) {
            if (offsetToHyperplane[i] > 0) {
                fingerprint |= (1l << i);
            }
        }
        return fingerprint;
    }
}
