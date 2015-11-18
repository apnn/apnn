package Eval;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.fcollection.FHashMap;
import io.github.htools.fcollection.FHashMapIntObject;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A generic class to compute an evaluation metric over a set of retrieved
 * nearest neighbor source documents for the collection of suspicious documents.
 * For each suspicious document the evaluation metric is based on the comparison
 * between the retrieved set of k-most nearest neighbors and a ground truth set
 * of k-most nearest neighbors. Both the retrieved set and ground truth should
 * be input as a SimilarityFile. When constructing a single ground truth file is
 * given, along with k which is the maximum rank considered (top-k).
 *
 * Important! The SimilarityFiles should be in order of descending similarity
 * score per suspicious document.
 *
 * @author Jeroen
 */
public abstract class Metric {

    public static Log log = new Log(Metric.class);
    public static boolean useIndexSimilarity = false;
    private ResultSet groundTruth;

    public Metric(ResultSet gt) {
        groundTruth = gt;
    }

    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "metric groundtruth results  --hdfs --ie -r [rank]");

        Datafile groundtruth = conf.getBoolean("hdfs", false)
                ? conf.getHDFSFile("groundtruth")
                : conf.getFSFile("groundtruth");
        ResultSet gt = loadFile(groundtruth);
        int ranks[] = conf.getInts("rank");
        if (ranks.length == 0) {
            ranks = new int[]{10};
        }
        //log.info("ranks %s", ArrayTools.toString(ranks));
        if (conf.containsKey("ie")) {
            useIndexSimilarity = true;
        }
        HPath path = conf.getBoolean("hdfs", false)
                ? conf.getHDFSPath("results")
                : conf.getFSPath("results");
        log.info("%s", path.getCanonicalPath());
        for (Datafile resultFile : path.getFiles()) {
            log.info("%s", resultFile.getCanonicalPath());
            ResultSet retrievedDocuments = loadFile(resultFile);
            for (String metricname : conf.get("metric").split(",")) {
                double scores[] = new double[ranks.length];
                Metric metric = get(metricname, gt);
                for (int i = 0; i < ranks.length; i++) {
                    int rank = ranks[i];
                    HashMap<Document, Double> scorePerDocument = metric.score(retrievedDocuments, rank);
                    double avgScore = metric.mean(scorePerDocument);
                    scores[i] = avgScore;
                    log.printf("%s n=%d %s@%3d=%f", resultFile.getName(), scorePerDocument.size(), metricname, rank, avgScore);
                }
                if (ranks.length > 1) {
                    log.printf("%s", ArrayTools.toString(scores, "\t"));
                }
            }
        }
    }

    public static Metric get(String metric, ResultSet groundtruth) throws Exception {
        switch (metric.toLowerCase()) {
            case "ndcg":
                return new NDCG(groundtruth);
            case "recall":
                return new Recall(groundtruth);
            case "precision":
                return new Precision(groundtruth);
            case "rprecision":
                return new RPrecision(groundtruth);
            case "krecall":
                return new KRecall(groundtruth);
            default:
                throw new Exception("unknown metric " + metric);
        }
    }

    public static double getSimilarity(SourceDocument d) {
        return (useIndexSimilarity) ? d.indexSimilarity : d.measureSimilarity;
    }

    /**
     * @param groundtruth the optimal retrieved set of nearest neighbors for a
     * given suspicious document
     * @param retrievedDocument the retrieved set of nearest neighbors for a
     * given suspicious document
     * @return the metrics score for the nearest neighbors for the retrieved
     * document vs the ground truth
     */
    public abstract double score(SuspiciousDocument groundtruth,
            SuspiciousDocument retrievedDocument, int k);

    /**
     * @param retrievedDocument
     * @return the score for a single document with its retrieved nearest
     * neighbors
     */
    public double score(SuspiciousDocument retrievedDocument, int k) {
        SuspiciousDocument gt = groundTruth.get(retrievedDocument.docid);
        if (gt != null && gt.relevantDocuments.size() > 0) {
            return score(gt, retrievedDocument, k);
        } else {
            return 0;
        }
    }

    public Map<String, SuspiciousDocument> getGroundTruth() {
        return Collections.unmodifiableMap(groundTruth);
    }

    /**
     * @param resultFile
     * @return a map of scored documents (id, score) for the retrieved nearest
     * neighbors in the given file.
     */
    public HashMap<Document, Double> score(ResultSet retrievedDocuments, int k) {
        HashMap<Document, Double> results = new HashMap();
        for (SuspiciousDocument suspiciousDoc : this.groundTruth.values()) {
            SuspiciousDocument scored = retrievedDocuments.get(suspiciousDoc.docid);
            if (scored != null) {
                double score = Metric.this.score(scored, k);
                //log.info("%s score %f", Metric.this.getClass().getCanonicalName(), score);
                results.put(scored, score);
            }
        }
        return results;
    }

    /**
     * @param scores map of scored documents (id, score)
     * @return the mean metric over the scored documents
     */
    public double mean(HashMap<Document, Double> scores) {
        return DoubleTools.mean(scores.values());
    }

    /**
     * @param file a SimilarityFile (e.g. ground truth or a results file)
     * @return a Set of the SuspiciousDocuments in the SimilarityFile with the
     * list of nearest neighbor SourceDocuments.
     */
    public static ResultSet loadFile(Datafile file) {
        file.setBufferSize(1000000);
        HashMapList<String, SourceDocument> sourceMap = new HashMapList(11100);
        SimilarityFile similarityFile = new SimilarityFile(file);
        for (SimilarityWritable similarity : similarityFile) {
            SourceDocument sd = new SourceDocument(similarity);
            sourceMap.add(similarity.id, sd);
        }

        ResultSet map = new ResultSet(11100); // prevent rehashing
        for (Map.Entry<String, ArrayList<SourceDocument>> entry : sourceMap.entrySet()) {
            ArrayList<SourceDocument> list = entry.getValue();
            Collections.sort(list, Collections.reverseOrder());
            SuspiciousDocument gt = new SuspiciousDocument(entry.getKey());
            for (int i = 0; i < list.size(); i++) {
                SourceDocument sd = list.get(i);
                sd.position = i + 1;
                gt.add(sd);
            }
            map.put(gt.docid, gt);
        }

        return map;
    }

    public static class ResultSet extends FHashMap<String, SuspiciousDocument> {

        public ResultSet() {
            super();
        }

        public ResultSet(int size) {
            super(size);
        }

        public ResultSet(Map<String, SuspiciousDocument> map) {
            super(map);
        }
    }

    public static class Document {

        public String docid;

        public String toString() {
            return "Doc " + docid;
        }
    }

    public static class SuspiciousDocument extends Document {

        public FHashMap<String, SourceDocument> relevantDocuments = new FHashMap();

        public SuspiciousDocument(String docid) {
            this.docid = docid;
        }

        public SourceDocument getSourceDocument(String sourceDocumentId) {
            return relevantDocuments.get(sourceDocumentId);
        }

        public SourceDocument getSourceDocument(SourceDocument sourceDocument) {
            return relevantDocuments.get(sourceDocument.docid);
        }

        /**
         * Add a nearest neighbor from the ground truth, unless already k
         * nearest neighbors were registered for this document.
         *
         * @param similarity
         */
        public void add(SourceDocument source) {
            relevantDocuments.put(source.docid, source);
        }
    }

    public static class SourceDocument extends Document implements Comparable<SourceDocument> {

        public int position;
        public double measureSimilarity;
        public double indexSimilarity;

        public SourceDocument(
                SimilarityWritable similarity) {
            this.docid = similarity.source;
            this.measureSimilarity = similarity.measureSimilarity;
            this.indexSimilarity = similarity.indexSimilarity;
        }

        public double getScore() {
            return getSimilarity(this);
        }

        @Override
        public int compareTo(SourceDocument o) {
            return Double.compare(getScore(), o.getScore());
        }
    }
}
