package RobustEval;

import RepackNYT.*;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
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
public class SwitchFDMScoresReduce extends Reducer<Text, ResultWritable, NullWritable, ResultWritable> {

    public static final Log log = new Log(SwitchFDMScoresReduce.class);

    @Override
    public void reduce(Text key, Iterable<ResultWritable> records, Context context) throws IOException, InterruptedException {
       log.info("key %s", key.toString());
        HashMap<String, Double> fdm = new HashMap();
       ArrayList<ResultWritable> results = new ArrayList();
       for (ResultWritable r : records) {
           if (r.run.equals("fdm")) {
               log.info("fdm %s %s %s %s %f", r.getKey(), r.query, r.run, r.id, r.score);
               fdm.put(r.getKey(), r.score);
           } else {
               log.info("res %s %s %s %s %f", r.getKey(), r.query, r.run, r.id, r.score);
               results.add(r.clone());
           }
       }
       for (ResultWritable r : results) {
           Double score = fdm.get(r.getKey());
           log.info("%s %s %s", r.query, r.id, score);
           r.score = score==null?-100000:score;
           context.write(NullWritable.get(), r);
       }
    }
}
