package RobustQlFdm;

import RepackRobust.*;
import TestGeneric.Document;
import TestGeneric.ContentExtractorRobust;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderContentTrec;
import io.github.htools.collection.ArrayMap;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.IntDoubleWritable;
import io.github.htools.hadoop.io.IntIntWritable;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.ByteTools;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class QlFdmReducer extends Reducer<IntDoubleWritable, Text, NullWritable, NullWritable> {

    public static final Log log = new Log(QlFdmReducer.class);
    Conf conf;
    Datafile out;
    int rank = 0;
    int query = 0;

    public void setup(Context context) throws IOException {
        Document.setContentExtractor(new ContentExtractorRobust());
        conf = ContextTools.getConfiguration(context);
        out = new Datafile(conf.getHDFSFile("output"));
        out.openWrite();
    }

    @Override
    public void reduce(IntDoubleWritable key, Iterable<Text> ids, Context context) throws IOException, InterruptedException {
        if (key.get() != query) {
            query = key.get();
            rank = 1;
        }
        for (Text id : ids) {
           out.printf("%d Q0 %s %d %f %s\n", query, id.toString(), rank++, key.getValue2(), "run");
        }
    }
    
    @Override
    public void cleanup(Context context) {
        out.closeWrite();
    }
}
