package RepackPAN11;

import TestGeneric.Tokenizer;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.search.ByteSearch;
import org.apache.hadoop.io.NullWritable;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class RepackMap extends Mapper<Object, String, NullWritable, NullWritable> {

    public static final Log log = new Log(RepackMap.class);
    Conf conf;
    Tokenizer tokenizer = new Tokenizer();
    HDFSPath outPath;

    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
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
                ArrayList<String> terms = tokenizer.tokenize(readAll);

                for (String term : terms) {
                     buffer.writeRaw("%s ", term);
                }
                //log.info("%s", terms);
                outputArchive.write(getDocNumber(archiveEntry.getName()), buffer.getSize(), buffer.getAsInputStream());
            //}
        }
        outputArchive.close();
    }
    
    static ByteSearch docNumber = ByteSearch.create("\\d+");
    public static String getDocNumber(String docname) {
        return Integer.toString(Integer.parseInt(docNumber.extract(docname)));
    }    
}
