package RepackPAN11;

import Vocabulary.*;
import TestGeneric.Tokenizer;
import TestGeneric.TokenizerRemoveStopwords;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.extract.Content;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.ByteTools;
import io.github.htools.type.TermVectorDouble;
import io.github.htools.type.TermVectorInt;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class RepackMap extends Mapper<Object, String, NullWritable, NullWritable> {

    public static final Log log = new Log(RepackMap.class);
    Conf conf;
    Tokenizer tokenizer = new TokenizerRemoveStopwords();
    Idf idf;
    TermID termid;
    HDFSPath outPath;

    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        idf = new Idf(conf.getHDFSFile("vocabulary"));
        termid = new TermID(conf.getHDFSFile("vocabulary"));
        outPath = conf.getHDFSPath("output");
    }

    @Override
    public void map(Object key, String value, Context context) throws IOException, InterruptedException {
        ArchiveFile inputArchive = ArchiveFile.getReader(conf, value);
        log.info("input %s", value);

        String filename = new HDFSPath(conf, value).getName();
        ArchiveFileWriter outputArchive = ArchiveFileWriter.getWriter(outPath.getFile(filename), 9);
        BufferDelayedWriter buffer = new BufferDelayedWriter();

        for (ArchiveEntry archiveEntry : (Iterable<ArchiveEntry>) inputArchive) {
            //log.info("file %s", archiveEntry.getName());
            //if (archiveEntry.getName().contains("source-document09146.txt")) {
                byte[] readAll = archiveEntry.readAll();
                byte[] copy = ArrayTools.clone(readAll);
                ArrayList<String> terms = tokenizer.tokenize(readAll);

                TermVectorDouble model = new TermVectorDouble(terms);
                for (Map.Entry<String, Double> entry : model.entrySet()) {
                    Double tf = entry.getValue();
                    double tfidf = tf * idf.get(entry.getKey());
                    if (!termid.containsKey(entry.getKey())) {
                        log.info("%b", model.containsKey("explic"));
                        log.info("failed key %s %s %s", archiveEntry.getName(), entry.getKey(), terms);
                        log.crash();
                    }
                    int termid = this.termid.get(entry.getKey());
                    buffer.writeRaw("%d\t%f\n", termid, tfidf);
                }
                //log.info("%s", terms);
                outputArchive.write(archiveEntry.getName(), buffer.getSize(), buffer.getAsInputStream());
            //}
        }
        outputArchive.close();
    }
}
