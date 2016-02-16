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

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class QlFdmMap extends Mapper<Object, Datafile, IntDoubleWritable, Text> {

    public static final Log log = new Log(QlFdmMap.class);
    Conf conf;
    HDFSPath qlPath;
    IntDoubleWritable outKey = new IntDoubleWritable();
    Text outValue = new Text();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        qlPath = conf.getHDFSPath("ql");
    }

    @Override
    public void map(Object key, Datafile fdmFile, Context context) throws IOException, InterruptedException {
        HashMap<String, Double> fdm = getFdm(fdmFile);
        ArrayMap<Double, String> retrieved = new ArrayMap();
        Datafile qlFile = qlPath.getFile(fdmFile.getName());
        log.info("file %s", qlFile.getCanonicalPath());
        int rank = 1;
        for (String line : qlFile.readLines()) {
            String[] part = line.split("\t");
            String id = part[1];
            retrieved.add(fdm.get(id), id);
            log.info("rank %d", rank);
            if (rank++ > conf.getInt("k", 0))
                break;
        }
        //qlFile.closeRead();
        outKey.set(Integer.parseInt(qlFile.getName()));
        for (Map.Entry<Double, String> entry : retrieved.descending()) {
            outKey.setValue2(entry.getKey());
            outValue.set(entry.getValue());
            context.write(outKey, outValue);
        }
    }
    
    public HashMap<String,Double> getFdm(Datafile fdmFile) throws IOException {
        HashMap<String, Double> results = new HashMap();
        log.info("fdmFile %s", fdmFile.getCanonicalPath());
        fdmFile.setBufferSize(1000000);
        for (String record : fdmFile.readLines()) {
            String[] part = record.split("\t");
            results.put(part[1], Double.parseDouble(part[2]));
            if (results.size() % 1000 == 0)
                log.info("fdm %d", results.size());
        }
        return results;
    }
}
