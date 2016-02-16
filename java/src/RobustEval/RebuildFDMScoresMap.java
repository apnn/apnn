package RobustEval;

import SimilarityFile.SimilarityWritable;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class RebuildFDMScoresMap extends Mapper<Object, ResultWritable, Text, SimilarityWritable> {

    public static final Log log = new Log(RebuildFDMScoresMap.class);
    Text outKey = new Text();
    SimilarityWritable outValue = new SimilarityWritable();

    @Override
    public void map(Object key, ResultWritable result, Context context) throws IOException, InterruptedException {
        result.get(outValue);
        if (outValue.measureSimilarity == 0.0)
            log.crash("query %s id %s", result.query, result.id);
        outKey.set(result.run);
        context.write(outKey, outValue);
    }
}
