package RepackTFIDF2;

import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTFIDF;
import VocabularyPAN.VocabularyFile;
import VocabularyPAN.VocabularyWritable;
import io.github.htools.fcollection.FHashSet;
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
    HDFSPath outPath;
    //FHashSet<String> singles;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        outPath = conf.getHDFSPath("output");
        reader = new DocumentReaderTFIDF();
        //singles = singleTerms(conf.getHDFSFile("vocabulary"));
    }

    @Override
    public void map(Object key, String value, Context context) throws IOException, InterruptedException {
        Datafile df = new Datafile(conf, value);
        df.setBufferSize(1000000);
        log.info("input %s", value);

        String filename = new HDFSPath(conf, value).getName();
        Datafile out = outPath.getFile(filename);
        out.setBufferSize(1000000);
        ArchiveFileWriter outputArchive = ArchiveFileWriter.getWriter(out, 9);
        BufferDelayedWriter buffer = new BufferDelayedWriter();

        for (Document doc : reader.iterableDocuments(df)) {
            TermVectorDouble model = (TermVectorDouble) doc.getModel();
            double magnitude = model.magnitude();
            for (Map.Entry<String, Double> entry : model.entrySet()) {
                //if (!singles.contains(entry.getKey())) {
                    buffer.writeRaw("%s\t%f\n", entry.getKey(), entry.getValue() / magnitude);
                //}
            }
            outputArchive.write(doc.getId(), buffer.getSize(), buffer.getAsInputStream());
        }
        outputArchive.close();
    }

    public FHashSet<String> singleTerms(Datafile df) {
        FHashSet<String> singles = new FHashSet();
        VocabularyFile vocabularyFile = new VocabularyFile(df);
        vocabularyFile.setBufferSize(1000000);
        int termid = 0;
        for (VocabularyWritable line : vocabularyFile) {
            if (line.documentFrequency < 2) {
                singles.add(termid + "");
            }
            termid++;
        }
        return singles;

    }
}
