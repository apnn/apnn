package Eval;

import Eval.EvalMap.Result;
import Eval.Metric.Document;
import Eval.Metric.GTMap;
import Eval.Metric.ResultSet;
import static Eval.Metric.loadFile;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.buffered.WritableComparable;
import io.github.htools.io.Datafile;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Every map receives one Value as input, that contains a unique combination of
 * a filename with a block of suspicious documents and a filename with a block
 * of source documents. The default operation is to do a brute force comparison
 * by reading all source documents into memory, and then the suspicious
 * documents are inspected one-at-a-time for matches in the index. The k-most
 * similar source documents are send to the reducer. This generic mapper can be
 * configured by setting the index, similarity function and k in the job, or
 * overridden to add functionality.
 *
 * @author jeroen
 */
public class EvalMap extends Mapper<String, String, Result, DoubleWritable> {

    public static final Log log = new Log(EvalMap.class);
    protected Conf conf;
    DoubleWritable outValue = new DoubleWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
    }

    @Override
    public void map(String key, String resultFile, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        Datafile resultDatafile = new Datafile(conf, resultFile);
        GTMap groundTruth = MetricAtK.loadFile(conf.getHDFSFile("groundtruth"));
        ResultSet retrievedDocuments = MetricAtK.loadResults(resultDatafile);
        Result outKey = new Result();
        outKey.run = resultDatafile.getDir().getName();
        for (String metricname : conf.get("metrics").split(",")) {
            double scores[] = new double[conf.getStrings("ranks").length];
            Metric metric = Metric.get(metricname, groundTruth);
            outKey.metric = metricname;
            if (metric instanceof MetricAtK) {
                for (int rank : conf.getInts("ranks")) {
                    outKey.rank = rank;
                    HashMap<Document, Double> scorePerDocument = ((MetricAtK) metric).score(retrievedDocuments, rank);
                    for (Map.Entry<Document, Double> entry : scorePerDocument.entrySet()) {
                        outKey.queryid = entry.getKey().queryid;
                        outValue.set(entry.getValue());
                        context.write(outKey, outValue);
                    }
                }
            } else {
                outKey.rank = 0;
                HashMap<Document, Double> scorePerDocument = ((MetricNoK) metric).score(retrievedDocuments);
                for (Map.Entry<Document, Double> entry : scorePerDocument.entrySet()) {
                    outKey.queryid = entry.getKey().queryid;
                    outValue.set(entry.getValue());
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class Result extends WritableComparable {

        String run;
        String metric;
        String queryid;
        int rank;

        @Override
        public int compareTo(Object o) {
            Result oo = (Result) o;
            int comp = run.compareTo(oo.run);
            if (comp == 0) {
                comp = metric.compareTo(oo.metric);
                if (comp == 0) {
                    comp = rank - oo.rank;
                }
            }
            return comp;
        }

        @Override
        public void write(BufferDelayedWriter writer) {
            writer.write(run);
            writer.write(metric);
            writer.write(rank);
            writer.write(queryid);
        }

        @Override
        public void readFields(BufferReaderWriter reader) {
            run = reader.readString();
            metric = reader.readString();
            rank = reader.readInt();
            queryid = reader.readString();
        }
        
        
    }
}
