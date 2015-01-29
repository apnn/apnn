package kba4sort;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import java.io.IOException;
import kba1raw.ReducerKeysDays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import streamcorpus.sentence.SentenceFile;

/**
 *
 * @author jeroen
 */
public class SortReducer extends Reducer<LongWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(SortReducer.class);
    Datafile df;
    SentenceFile sf;
    int sequence = 0;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String date = DayPartitioner.getDate(conf, ContextTools.getTaskID(context));
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        df = outdir.getFile(date);
        log.info("setup %s %b %d", df.getCanonicalPath(), df.exists(), df.exists() ? df.getLength() : -1);
        sf = new SentenceFile(df);
        sf.openWrite();
    }

    @Override
    public void reduce(LongWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable s : values) {
            int day = ReducerKeysDays.getDay(s.creationtime);
            s.id = (day << 22) | sequence++;
            s.write(sf);
        }
    }

    @Override
    public void cleanup(Context context) {
        sf.closeWrite();
    }
}
