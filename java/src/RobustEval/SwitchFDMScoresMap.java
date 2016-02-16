package RobustEval;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.DelayedWritable;
import io.github.htools.io.Datafile;
import org.apache.hadoop.io.Text;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class SwitchFDMScoresMap extends Mapper<String, String, Text, ResultWritable> {

    public static final Log log = new Log(SwitchFDMScoresMap.class);
    Text outKey = new Text();

    @Override
    public void map(String type, String filename, Context context) throws IOException, InterruptedException {
        log.info("type %s file %s", type, filename);
        Conf conf = ContextTools.getConfiguration(context);
        Datafile df = new Datafile(conf, filename);
        ResultWritable outValue = new ResultWritable();
        outValue.run = type.equals("fdm") ? type : df.getName();
        log.info("run %s", outValue.run);
        df.setBufferSize(1000000);
        SimilarityFile sim = new SimilarityFile(df);
        for (SimilarityWritable w : sim) {
            //log.info("%s %s %s %s", w.query, w.source, w.indexSimilarity, w.measureSimilarity);
            if (w.source.length() > 0) {
                outValue.set(w);
                outKey.set(outValue.getKey());
                context.write(outKey, outValue);
            }
        }
    }
}
