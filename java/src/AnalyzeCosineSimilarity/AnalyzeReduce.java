package AnalyzeCosineSimilarity;

import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Lazy implementation, the result is just viewable in the Job's log.
 * @author jeroen
 */
public class AnalyzeReduce extends Reducer<Text, Result, NullWritable, NullWritable> {

    public static final Log log = new Log(AnalyzeReduce.class);
    ArrayList<Double> contribution[] = new ArrayList[5];
    ArrayList<Double> similarityNN;

    @Override
    public void setup(Context context) {
       similarityNN = new ArrayList();
       for (int i = 0; i < contribution.length; i++)
           contribution[i] = new ArrayList();
    }

    @Override
    public void reduce(Text key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        Result top = null;
        for (Result t : values) {
            if (top == null || top.similarity < t.similarity) {
                top = t.clone();
            }
        }
        top.map.descending();
        //log.printf("%s", top.toString());
        for (int i = 0; i < contribution.length && i < top.map.size(); i++) {
            contribution[i].add( top.map.getKey(i) / top.magnitude );
        }
        similarityNN.add(top.similarity);
    }

    @Override
    public void cleanup(Context context) {
        for (int i = 0; i < contribution.length; i++) {
            log.info("rank %d contribution %f", i, DoubleTools.mean(contribution[i]));
            log.info("average similarity to NN %f", DoubleTools.mean(similarityNN));
        }
    }
}
