package NYTEval;

import SimilarityFile.SimilarityWritable;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.buffered.Writable;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class RefineMap extends Mapper<Object, SimilarityWritable, IntWritable, RefineMap.Result> {

    public static final Log log = new Log(RefineMap.class);
    Conf conf;
    IntWritable key = new IntWritable();
    Result value = new Result();
    TreeMap<Integer, Integer> idMap;
    int queryid = -1;
    int maxsource = -1;
    double maxsim = -1;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        idMap = readIds();
        value.run = ContextTools.getInputPath(context).getName();
    }

    @Override
    public void map(Object key, SimilarityWritable w, Context context) throws IOException, InterruptedException {
        log.info("%s", w.query);
        int q = Integer.parseInt(w.query);
        if (queryid != q && maxsim != -1) {
            cleanup(context);
            queryid = q;
        }
        if (w.measureSimilarity > maxsim) {
            maxsim = w.measureSimilarity;
            maxsource = Integer.parseInt(w.source);
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        if (queryid > -1) {
            value.queryid = queryid;
            value.sourceid = maxsource;
            setKey();
            context.write(key, value);
            maxsource = -1;
            maxsim = -1;
        }
    }

    public void setKey() {
        int sourceline = idMap.ceilingEntry(value.sourceid).getValue();
        int queryline = idMap.ceilingEntry(value.queryid).getValue();
        key.set(queryline * idMap.size() + sourceline);
    }

    public static class Result extends Writable {
        String run;
        public int sourceid;
        public int queryid;

        @Override
        public void write(BufferDelayedWriter writer) {
            writer.write(run);
            writer.write(queryid);
            writer.write(sourceid);
        }

        @Override
        public void readFields(BufferReaderWriter reader) {
            run = reader.readString();
            queryid = reader.readInt();
            sourceid = reader.readInt();
        }
    }

    public TreeMap<Integer, Integer> readIds() {
        TreeMap<Integer, Integer> map = new TreeMap();
        for (String line : conf.getHDFSFile("ids").readLines()) {
            String[] part = line.split("\\s+");
            map.put(Integer.parseInt(part[2]), map.size());
        }
        return map;
    }
}
