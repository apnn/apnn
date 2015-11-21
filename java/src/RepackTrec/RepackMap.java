package RepackTrec;

import TestGeneric.Document;
import TestGeneric.TokenizerTrec;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderContentTrec;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveFileWriter;
import org.apache.hadoop.io.NullWritable;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class RepackMap extends Mapper<Object, Datafile, NullWritable, NullWritable> {

    public static final Log log = new Log(RepackMap.class);
    Conf conf;
    DocumentReader reader;
    HDFSPath outPath;

    public void setup(Context context) throws IOException {
        Document.setTokenizer( new TokenizerTrec() );
        conf = ContextTools.getConfiguration(context);
        outPath = conf.getHDFSPath("output");
        reader = new DocumentReaderContentTrec();
    }

    @Override
    public void map(Object key, Datafile value, Context context) throws IOException, InterruptedException {
        BufferDelayedWriter buffer = new BufferDelayedWriter();
        String name = value.getName().endsWith(".tar.lz4")
                ? value.getName() : value.getName() + ".tar.lz4";
        ArchiveFileWriter outputArchive = ArchiveFileWriter.getWriter(outPath.getFile(name), 9);

        log.info("file %s", value.getCanonicalPath());
        for (Document doc : reader.iterableDocuments(value)) {
            for (String term : doc.getTerms()) {
                buffer.writeRaw("%s ", term);
            }
            outputArchive.write(doc.docid, buffer.getSize(), buffer.getAsInputStream());
        }
        outputArchive.close();
    }
}
