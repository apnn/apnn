package RepackTFIDFNYT;

import TestGeneric.ContentExtractorNYT;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTerms;
import VocabularyPAN.Idf;
import VocabularyPAN.TermID;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

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
        log.info("setup");
        conf = ContextTools.getConfiguration(context);
        idf = new Idf(conf.getHDFSFile("vocabulary"));
        termid = new TermID(conf.getHDFSFile("vocabulary"));
        outPath = conf.getHDFSPath("output");
        reader = new DocumentReaderTerms();
        Document.setContentExtractor(new ContentExtractorNYT());
    }

    @Override
    public void map(Object key, String value, Context context) throws IOException, InterruptedException {
        Datafile df = new Datafile(conf, value);
        df.setBufferSize(1000000);
        log.info("input %s", value);

        String filename = new HDFSPath(conf, value).getName();
        Datafile out = outPath.getFile(filename);
        ArchiveFileWriter outputArchive = ArchiveFileWriter.getWriter(out, 9);
        BufferDelayedWriter buffer = new BufferDelayedWriter();

        for (Document doc : reader.iterableDocuments(df)) {
            ArrayList<String> terms = doc.getTerms();
            //log.info("%s %s", doc.getId(), terms);
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
