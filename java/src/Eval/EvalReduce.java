package Eval;

import Eval.EvalMap.Result;
import Eval.Metric.GTMap;
import io.github.htools.collection.ArrayMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.lib.StrTools;
import io.github.htools.type.Pair;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * FDM is scored here as query likelihood like in the original, however Zhai's
 * Dirichlet smoothed LM notation is rank equivalent and faster.
 *
 * @author jeroen
 */
public class EvalReduce extends Reducer<Result, DoubleWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(EvalReduce.class);
    Conf conf;
    GTMap groundTruth;
    Datafile outfile;
    HashMap<String, HashMap<String, ArrayMap<Integer, Double>>> scores = new HashMap();
    
    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        groundTruth = MetricAtK.loadFile(conf.getHDFSFile("groundtruth"));
    }

    @Override
    public void reduce(Result key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double score = 0;
        for (DoubleWritable d : values) {
            log.info("%s %s@%d %s %f", key.run, key.metric, key.rank, key.queryid, d.get());
            score += d.get();
        }
        score /= groundTruth.size();
        HashMap<String, ArrayMap<Integer, Double>> run = scores.get(key.run);
        if (run == null) {
            run = new HashMap();
            scores.put(key.run, run);
        }
        ArrayMap<Integer, Double> metric = run.get(key.metric);
        if (metric == null) {
            metric = new ArrayMap();
            run.put(key.metric, metric);
        }
        metric.add(key.rank, score);
        
    }
    
    public void cleanup(Context context) {
        outfile = conf.getHDFSFile("output");
        outfile.openWrite();
        for (Map.Entry<String, HashMap<String, ArrayMap<Integer, Double>>> entry : scores.entrySet()) {
            String run = entry.getKey();
            HashMap<String, ArrayMap<Integer, Double>> runresults = entry.getValue();
            for (Map.Entry<String, ArrayMap<Integer, Double>> entry2 : runresults.entrySet()) {
                String metric = entry2.getKey();
                ArrayMap<Integer, Double> metricresults = entry2.getValue();
                outfile.printf("%s %s %s\n", run, metric, metricresults.keySet());
                for (double score : metricresults.values() ) {
                    outfile.printf("%.4f ", score);
                }
                outfile.print("\n");
            }
        }
        outfile.closeWrite();
    }
}
