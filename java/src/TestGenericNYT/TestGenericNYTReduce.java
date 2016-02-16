package TestGenericNYT;

import SimilarityFile.IndexSimilarity;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import io.github.htools.collection.TopK;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
public class TestGenericNYTReduce extends Reducer<Text, Candidate, NullWritable, SimilarityWritable> {

    public static final Log log = new Log(TestGenericNYTReduce.class);
    enum REDUCE {
        RETRIEVED,
        SCANNED,
        RETURNED
    }
    Conf conf;
    Comparator<SimilarityWritable> comparator;
    // the number of most similar documents to keep, configurable as "topk".
    int resultSize;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        resultSize = TestGenericNYTJob.getTopK(conf);
        comparator = getComparator();
    }

    @Override
    public void reduce(Text key, Iterable<Candidate> values, Context context) throws IOException, InterruptedException {
        String query = key.toString();
        // a map that automatically keeps only the items with the top-k highest keys
        TopK<Candidate> topk = new TopK(resultSize, comparator);

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

        context.getCounter(REDUCE.RETURNED).increment(list.size());
        // write the top-k most similar documents to file
        for (Candidate c : list) {
            context.write(NullWritable.get(), c);
        }
    }
    
    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    }
}
