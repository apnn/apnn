package NYTEval;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class EvalReduce extends Reducer<Text, ResultWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(EvalReduce.class);
    Conf conf;
    HDFSPath outPath;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        outPath = conf.getHDFSPath("output");
    }

    @Override
    public void reduce(Text doc, Iterable<ResultWritable> results, Context context) throws IOException, InterruptedException {
        double recall = 0;
        double precision = 0;
        double f = 0;
        int count = 0;
        for (ResultWritable w : results) {
            recall += w.recall;
            precision += w.precision;
            if (w.recall + w.precision > 0)
               f += 2 * w.recall * w.precision / (w.recall + w.precision);
            count++;
        }
        recall /= count;
        precision /= count;
        f /= count;
        double macrof = 2 * recall * precision / (recall + precision);
        Datafile df = outPath.getFile(doc.toString());
        df.openWrite();
        df.printf("recall\t%f\nprecision\t%f\nmicrof\t%f\nmacrof\t%f\n", recall, precision, f, macrof);
        df.closeWrite();
    }
}
