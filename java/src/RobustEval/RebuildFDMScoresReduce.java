package RobustEval;

import RobustEval.*;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
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
public class RebuildFDMScoresReduce extends Reducer<Text, SimilarityWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RebuildFDMScoresReduce.class);
    Conf conf;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
    }

    @Override
    public void reduce(Text key, Iterable<SimilarityWritable> records, Context context) throws IOException, InterruptedException {
        Datafile df = conf.getHDFSPath("output").getFile(key.toString());
        df.setBufferSize(1000000);
        df.openWrite();
        ArrayList<SimilarityWritable> list = new ArrayList();
        for (SimilarityWritable r : records) {
            list.add(r.clone());
        }
        Collections.sort(list, new Comp());
        int rank = 1;
        String prevquery = "";
        for (SimilarityWritable r : list) {
            if (!r.query.equals(prevquery)) {
                prevquery = r.query;
                rank = 1;
            }
            df.printf("%s Q0 %s %d %e %s\n", r.query, r.source, rank++, r.measureSimilarity, "run");
        }
        df.closeWrite();
    }
    
    class Comp implements Comparator<SimilarityWritable> {

        @Override
        public int compare(SimilarityWritable o1, SimilarityWritable o2) {
            int comp = o1.query.compareTo(o2.query);
            if (comp == 0) {
                comp = Double.compare(o2.measureSimilarity, o1.measureSimilarity);
            }
            return comp;
        }
        
    }
}
