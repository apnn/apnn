package RepackRobust;

import TestGeneric.ContentExtractorRobust;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderContentTrec;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RepackMap extends Mapper<Object, Datafile, NullWritable, NullWritable> {

    public static final Log log = new Log(RepackMap.class);
    Conf conf;
    DocumentReader reader;
    HDFSPath outPath;

    public void setup(Context context) throws IOException {
        Document.setContentExtractor(new ContentExtractorRobust());
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

        //log.info("file %s", value.getCanonicalPath());
        for (Document doc : reader.iterableDocuments(value)) {
            byte[] content = doc.getContent();
            buffer.writeRaw(ByteTools.toBytes(content, 0, content.length));
            outputArchive.write(doc.docid, buffer.getSize(), buffer.getAsInputStream());
        }
        outputArchive.close();
    }
}
