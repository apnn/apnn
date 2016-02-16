package TestGenericRobust;

import SimilarityFile.IndexSimilarity;
import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import io.github.htools.collection.TopK;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class TestGenericFDMReduce extends Reducer<Text, Candidate, NullWritable, NullWritable> {

    public static final Log log = new Log(TestGenericFDMReduce.class);
    enum REDUCE {
        RETRIEVED,
        SCANNED,
        RETURNED
    }
    Conf conf;
    Comparator<SimilarityWritable> comparator;
    // the number of most similar documents to keep, configurable as "topk".
    int resultSize;
    int scanSize;
    ArrayList<Double> cosineerror = new ArrayList();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        resultSize = TestGenericFDMJob.getTopK(conf);
        scanSize = resultSize;
        comparator = getComparator();
    }

    @Override
    public void reduce(Text key, Iterable<Candidate> values, Context context) throws IOException, InterruptedException {
        String query = key.toString();
        HashMap<String, Double> fdmScores = this.readFDMScores(query);
        // a map that automatically keeps only the items with the top-k highest keys
        TopK<Candidate> topk = new TopK(scanSize, comparator);

        // add all similarities for a given suspicious document (key) to a topkmap
        // to select only the top-k most similar source documents
        for (Candidate value : values) {
            context.getCounter(REDUCE.RETRIEVED).increment(1);
            value.measureSimilarity = fdmScores.get(value.source);
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
        SimilarityFile similarityFile = new SimilarityFile(conf.getHDFSPath("output").getFile(query));
        similarityFile.openWrite();
        for (Candidate c : list) {
            log.info("%s %s %f %f", c.query, c.source, c.indexSimilarity, c.measureSimilarity);
            c.write(similarityFile);
        }
        similarityFile.closeWrite();
    }

    public HashMap<String, Double> readFDMScores(String query) {
        HashMap<String, Double> scores = new HashMap();
        SimilarityFile similarityFile = new SimilarityFile(conf.getHDFSPath("fdmscores").getFile(query));
        for (SimilarityWritable s : similarityFile) {
            scores.put(s.source, s.measureSimilarity);
        }
        return scores;
    }
    
    public void setScanSize(int scanSize) {
        this.scanSize = scanSize;
    }
    
    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    }
    
    public ArrayList<Candidate> finalizeList(ArrayList<Candidate> list, int resultSize) {
        ArrayList<Candidate> result = new ArrayList();
        if (list.size() > resultSize) {
            result = list;
        } else {
            double value = list.get(resultSize - 1).indexSimilarity;
            for (int i = 0; i < resultSize ; i++) {
                result.add(list.get(i));
            }
            for (int i = resultSize; i < list.size() && list.get(i).indexSimilarity == value; i++) {
                result.add(list.get(i));
            }
        }
        Collections.sort(result, Collections.reverseOrder(MeasureSimilarity.singleton));
        return result;
    }
}
