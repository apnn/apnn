package TestGenericMR;

import SimilarityFile.IndexSimilarity;
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
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
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
public class TestGenericReduce extends Reducer<Text, Candidate, NullWritable, NullWritable> {

    public static final Log log = new Log(TestGenericReduce.class);
    enum REDUCE {
        RETRIEVED,
        SCANNED,
        RETURNED,
        MEANCOSINEERROR
    }
    protected Conf conf;
    SimilarityFile similarityFile;
    Comparator<SimilarityWritable> comparator;
    // the number of most similar documents to keep, configurable as "topk".
    int resultSize;
    int scanSize;
    ArrayList<Double> cosineerror = new ArrayList();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        resultSize = TestGenericJob.getTopK(conf);
        scanSize = resultSize;
        comparator = getComparator();
        setupOutput(context);
    }
    
    public void setupOutput(Context context) {
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
    public void reduce(Text key, Iterable<Candidate> values, Context context) throws IOException, InterruptedException {

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
        double abserror = 0;
        for (Candidate c : list) {
            log.info("%s %s %f %f", c.query, c.source, c.indexSimilarity, c.measureSimilarity);
            writeSimilarity(c);
            abserror += Math.abs(c.indexSimilarity - c.measureSimilarity);
        }
        
        this.cosineerror.add(abserror / list.size());
    }

    public void setScanSize(int scanSize) {
        this.scanSize = scanSize;
    }
    
    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    }
    
    public ArrayList<Candidate> finalizeList(ArrayList<Candidate> list, int resultSize) {
        ArrayList<Candidate> result = new ArrayList();
        if (list.size() <= resultSize) {
            return list;
        } else {
            double value = list.get(resultSize - 1).indexSimilarity;
            for (int i = 0; i < resultSize ; i++) {
                result.add(list.get(i));
            }
            for (int i = resultSize; i < list.size() && list.get(i).indexSimilarity == value; i++) {
                result.add(list.get(i));
            }
        }
        return result;
    }
    
    @Override
    public void cleanup(Context context) {
        closeOutput();
        double meanerror = DoubleTools.mean(this.cosineerror);
        context.getCounter(REDUCE.MEANCOSINEERROR).setValue((long)(1000000 * meanerror));
    }

    public void closeOutput() {
        similarityFile.closeWrite();
    }
    
    public void writeSimilarity(Candidate candidate) throws IOException {
        candidate.write(similarityFile);
    }

    public Conf getConf() {
        return conf;
    }    
}
