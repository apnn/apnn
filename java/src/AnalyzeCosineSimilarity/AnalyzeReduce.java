package AnalyzeCosineSimilarity;

import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author jeroen
 */
public class AnalyzeReduce extends Reducer<IntWritable, Result, NullWritable, NullWritable> {

    public static final Log log = new Log(AnalyzeReduce.class);
    ArrayList<Double> simpos[] = new ArrayList[5];
    ArrayList<Double> sim;

    @Override
    public void setup(Context context) {
       sim = new ArrayList();
       for (int i = 0; i < simpos.length; i++)
           simpos[i] = new ArrayList();
    }

    @Override
    public void reduce(IntWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        Result top = null;
        for (Result t : values) {
            if (top == null || top.similarity < t.similarity) {
                top = t.clone();
            }
        }
        top.map.descending();
        log.printf("%s", top.toString());
        for (int i = 0; i < simpos.length && i < top.map.size(); i++) {
            simpos[i].add( top.map.getKey(i) / top.magnitude );
        }
        sim.add(top.similarity);
    }

    @Override
    public void cleanup(Context context) {
        for (int i = 0; i < simpos.length; i++) {
            log.info("%d %f", i, DoubleTools.mean(simpos[i]));
            log.info("%f", DoubleTools.mean(sim));
        }
    }
}
