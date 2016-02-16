package Canopy;

import SimilarityFile.IndexSimilarity;
import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGenericMR.TestGenericJob;
import io.github.htools.collection.HashMap3;
import io.github.htools.collection.HashMapList;
import io.github.htools.collection.OrderedReverseQueueMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.type.KV;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class CanopyReduce extends Reducer<Text, SimilarityWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(CanopyReduce.class);

    enum REDUCE {

        RETRIEVED,
        SCANNED,
        RETURNED,
        MEANCOSINEERROR
    }
    Conf conf;
    SimilarityFile similarityFile;
    Comparator<SimilarityWritable> comparator;
    // the number of most similar documents to keep, configurable as "topk".
    int scanSize;
    HashMapList<String, String> canopies = new HashMapList();
    double T1;
    double T2;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        scanSize = TestGenericJob.getTopK(conf);
        T1 = CanopyGenericJob.getT1(conf);
        T2 = CanopyGenericJob.getT2(conf);

        // setup a single SimilarityFile that contains the k-most similar source
        // documents for a given suspicious document
        similarityFile = new SimilarityFile(new Datafile(conf, conf.get("output")));
        similarityFile.openWrite();
    }

    @Override
    public void reduce(Text key, Iterable<SimilarityWritable> values, Context context) throws IOException, InterruptedException {

        // a map that automatically keeps only the items with the top-k highest keys
        if (key.toString().startsWith("-")) {
            String source = key.toString().substring(1);
            boolean bound = false;
            for (SimilarityWritable w : values) {
                if (!w.query.equals(source) && w.measureSimilarity < T2) {
                    bound = true;
                }
                if (canopies.containsKey(source)) {
                    canopies.add(w.query, source);
                }
            }
            if (!bound) {
                canopies.add(source, source);
            }
            //log.info("%s %d", source, canopies.size());
        } else {
            String query = key.toString();
            HashMap3<String, Double, Double> measures = new HashMap3();
            OrderedReverseQueueMap<Double, String> topCanopies = new OrderedReverseQueueMap();
            for (SimilarityWritable s : values) {
                measures.put(s.source, 1-s.indexSimilarity, 1-s.measureSimilarity);
                //log.info("%s %s %f %f", query, s.source, s.indexSimilarity, s.measureSimilarity);
                if (s.indexSimilarity < T1) {
                    ArrayList<String> list = canopies.get(s.source);
                    if (list != null) {
                        topCanopies.add(s.indexSimilarity, s.source);
                    }
                }
            }
            //log.info("%s %d %d", query, topCanopies.size(), measures.size());
            HashSet<String> members = new HashSet();
            while (topCanopies.size() > 0) {
                String canopy = topCanopies.poll().getValue();
                ArrayList<String> canopyList = canopies.get(canopy);
                if (canopyList != null) {
                    members.addAll(canopyList);
                }
            }
            CandidateList result = new CandidateList(scanSize, getComparator());
            for (String member : members) {
                Candidate w = new Candidate();
                w.query = key.toString();
                w.source = member;
                KV<Double, Double> measure = measures.get(member);
                w.indexSimilarity = measure.key;
                w.measureSimilarity = measure.value;
                result.add(w);
            }
            int i = 0;
            for (Candidate w : result.sorted()) {
                if (i++ >= scanSize) {
                    break;
                }
                writeSimilarity(w);
            }
        }
    }

    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    }

    @Override
    public void cleanup(Context context) {
        similarityFile.closeWrite();
    }

    public void writeSimilarity(Candidate candidate) throws IOException {
        candidate.write(similarityFile);
    }
}
