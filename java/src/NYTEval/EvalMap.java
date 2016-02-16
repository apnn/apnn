package NYTEval;

import io.github.htools.lib.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class EvalMap extends Mapper<Object, ResultWritable, Text, ResultWritable> {

    public static final Log log = new Log(EvalMap.class);
    Text outKey = new Text();

    @Override
    public void map(Object key, ResultWritable line, Context context) throws IOException, InterruptedException {
        outKey.set(line.run);
        context.write(outKey, line);
    }
}
