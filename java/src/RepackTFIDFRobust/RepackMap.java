package RepackTFIDFRobust;

import TestGeneric.ContentExtractorRobust;
import VocabularyPAN.*;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTerms;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.ByteTools;
import io.github.htools.type.TermVectorDouble;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class RepackMap extends Mapper<Object, String, NullWritable, NullWritable> {

    public static final Log log = new Log(RepackMap.class);
    Conf conf;
    DocumentReader reader;
    Idf idf;
    TermID termid;
    HDFSPath outPath;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        idf = new Idf(conf.getHDFSFile("vocabulary"));
        termid = new TermID(conf.getHDFSFile("vocabulary"));
        log.info("individual %d %s", termid.size(), termid.get("individual"));
        log.info("florence %d %s", termid.size(), termid.get("florence"));
        outPath = conf.getHDFSPath("output");
        reader = new DocumentReaderTerms();
        Document.setContentExtractor(new ContentExtractorRobust());
    }

    @Override
    public void map(Object key, String value, Context context) throws IOException, InterruptedException {
        Datafile df = new Datafile(conf, value);
        log.info("input %s", value);

        String filename = new HDFSPath(conf, value).getName();
        ArchiveFileWriter outputArchive = ArchiveFileWriter.getWriter(outPath.getFile(filename), 9);
        BufferDelayedWriter buffer = new BufferDelayedWriter();

        for (Document doc : reader.iterableDocuments(df)) {
            ArrayList<String> terms = doc.getTerms();
            log.info("%s %s", ByteTools.toString(doc.getContent()), terms);
            TermVectorDouble model = new TermVectorDouble(terms);
            for (Map.Entry<String, Double> entry : model.entrySet()) {
                String term = entry.getKey();
                Double tf = entry.getValue();
                double tfidf = tf * idf.get(term);
                Integer termid = this.termid.get(term);
                if (termid == null)
                    log.fatal("term %s", term);
                buffer.writeRaw("%d\t%f\n", termid, tfidf);
            }
            //log.info("%s", terms);
            outputArchive.write(doc.getId(), buffer.getSize(), buffer.getAsInputStream());
            //}
        }
        outputArchive.close();
    }
}
