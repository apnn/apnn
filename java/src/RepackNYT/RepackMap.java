package RepackNYT;

import TestGeneric.ContentExtractorNYT;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderContent;
import io.github.htools.extract.ExtractChannel;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

/**
 * writes a repacked archive for the mapped input file, and send the
 * annotated labels to the reducer to compile a ground truth file.
 */
public class RepackMap extends Mapper<Object, Datafile, Text, Text> {

    public static final Log log = new Log(RepackMap.class);
    Conf conf;
    DocumentReader reader;
    HDFSPath outPath;
    ContentExtractorNYT extractor = new ContentExtractorNYT();
    HashSet<String> ids;

    @Override
    public void setup(Context context) throws IOException {
        Document.setContentExtractor(extractor);
        conf = ContextTools.getConfiguration(context);
        outPath = conf.getHDFSPath("output");
        reader = new DocumentReaderContent();
        ids = getIds();
    }

    @Override
    public void map(Object key, Datafile value, Context context) throws IOException, InterruptedException {
        BufferDelayedWriter buffer = new BufferDelayedWriter();
        String name = value.getName().endsWith(".tar.lz4")
                ? value.getName() : value.getName() + ".tar.lz4";
        ArchiveFileWriter outputArchive = ArchiveFileWriter.getWriter(outPath.getFile(name), 9);

        log.info("file %s", value.getCanonicalPath());
        for (Document doc : reader.iterableDocuments(value)) {
            // reconstructed this later after obtaining a file of ids, if an ids file is
            // not available, only process the documents that have annotated labels.
            if (ids.contains(doc.getId())) {
                byte[] content = doc.getContent();
                //log.info("%s %s", doc.docid, ByteTools.toString(content));
                buffer.writeRaw(ByteTools.toBytes(content, 0, content.length));
                outputArchive.write(doc.docid, buffer.getSize(), buffer.getAsInputStream());
                ExtractChannel labels = extractor.lastDocument.get("label");
                for (String label : labels.getTerms()) {
                    context.write(new Text(doc.getId()), new Text(label));
                }
            }
        }
        outputArchive.close();
    }


    public HashSet<String> getIds() {
        HashSet<String> ids = new HashSet(1632274);
        Datafile ids1 = conf.getHDFSFile("ids");
        for (String id : ids1.readLines()) {
            ids.add(id);
        }
        return ids;
    }
}
