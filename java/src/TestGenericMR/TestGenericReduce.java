package TestGenericMR;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.TopKMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.Map;
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
public class TestGenericReduce extends Reducer<IntWritable, SimilarityWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(TestGenericReduce.class);
    Conf conf;
    SimilarityFile similarityFile;
    // the number of most similar documents to keep, configurable as "topk".
    int k;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        k = conf.getInt("topk", 100);

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
    public void reduce(IntWritable key, Iterable<SimilarityWritable> values, Context context) throws IOException, InterruptedException {

        // a map that automatically keeps only the items with the top-k highest keys
        TopKMap<Double, SimilarityWritable> topk = new TopKMap(k);

        // add all similarities for a given suspicious document (key) to a topkmap
        // to select only the top-k most similar source documents
        for (SimilarityWritable value : values) {
            if (topk.wouldBeAdded(value.score)) {
                topk.add(value.score, value.clone());
            }
        }

        // write the top-k most similar documents to file
        for (Map.Entry<Double, SimilarityWritable> entry : topk.sorted()) {
            writeSimilarity(entry.getValue());
        }
    }

    @Override
    public void cleanup(Context context) {
        similarityFile.closeWrite();
    }

    public void writeSimilarity(SimilarityWritable similarityWritable) throws IOException {
        similarityWritable.write(similarityFile);
    }

    public Conf getConf() {
        return conf;
    }
}
