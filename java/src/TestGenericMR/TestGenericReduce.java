package TestGenericMR;

import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import io.github.htools.collection.TopK;
import io.github.htools.collection.TopKMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class TestGenericReduce extends Reducer<IntWritable, Candidate, NullWritable, NullWritable> {

    public static final Log log = new Log(TestGenericReduce.class);
    enum REDUCE {
        RETRIEVED,
        SCANNED,
        RETURNED
    }
    Conf conf;
    SimilarityFile similarityFile;
    Comparator<SimilarityWritable> comparator;
    // the number of most similar documents to keep, configurable as "topk".
    int resultSize;
    int scanSize;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        resultSize = TestGenericJob.getTopK(conf);
        scanSize = resultSize;
        comparator = getComparator();

        // setup a single SimilarityFile that contains the k-most similar source
        // documents for a given suspicious document
        if (conf.getNumReduceTasks()== 1) {
            similarityFile = new SimilarityFile(new Datafile(conf, conf.get("output")));
        } else {
            // supports multiple reducers by using the output as a directory
            // that will contain part.00000 files.
            HDFSPath outPath = new HDFSPath(conf, conf.get("output"));
            String filename = sprintf("part.%05d", ContextTools.getTaskID(context));
            similarityFile = new SimilarityFile(outPath.getFile(filename));
        }
        similarityFile.openWrite();
    }

    @Override
    public void reduce(IntWritable key, Iterable<Candidate> values, Context context) throws IOException, InterruptedException {

        // a map that automatically keeps only the items with the top-k highest keys
        TopK<Candidate> topk = new TopK(scanSize, comparator);

        // add all similarities for a given suspicious document (key) to a topkmap
        // to select only the top-k most similar source documents
        for (Candidate value : values) {
            context.getCounter(REDUCE.RETRIEVED).increment(1);
            if (topk.wouldBeAdded(value)) {
                topk.add(value.clone());
            }
        }
        context.getCounter(REDUCE.SCANNED).increment(topk.size());

        ArrayList<Candidate> list = new ArrayList(topk);
        Collections.sort(list, Collections.reverseOrder(comparator));
        list = finalizeList(list, resultSize);
        context.getCounter(REDUCE.RETURNED).increment(list.size());
        // write the top-k most similar documents to file
        for (Candidate c : list) {
            log.info("%d %d %f %f", c.id, c.source, c.indexSimilarity, c.measureSimilarity);
            writeSimilarity(c);
        }
    }

    public void setScanSize(int scanSize) {
        this.scanSize = scanSize;
    }
    
    public Comparator<SimilarityWritable> getComparator() {
        return MeasureSimilarity.singleton;
    }
    
    public ArrayList<Candidate> finalizeList(ArrayList<Candidate> list, int k) {
        ArrayList<Candidate> result = new ArrayList();
        if (list.size() <= k) {
            return list;
        } else {
            double value = list.get(k - 1).measureSimilarity;
            for (int i = 0; i < k ; i++) {
                result.add(list.get(i));
            }
            for (int i = k; i < list.size() && list.get(i).measureSimilarity == value; i++) {
                result.add(list.get(i));
            }
        }
        return result;
    }
    
    @Override
    public void cleanup(Context context) {
        similarityFile.closeWrite();
    }

    public void writeSimilarity(Candidate candidate) throws IOException {
        candidate.write(similarityFile);
    }

    public Conf getConf() {
        return conf;
    }    
}
