package PanDetection;

import PanDetection.AnnChunk.Passage;
import TestGeneric.ContentExtractorPAN;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderContent;
import TestGenericMR.SourceQueryPairInputFormat.Pair;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class PanMap extends Mapper<Integer, Pair, Text, Passage> {

    public static final Log log = new Log(PanMap.class);
    protected Conf conf;
    protected DocumentReader documentreader;
    protected AnnChunk index;
    public enum COUNTER { ADDCOUNT, ADDTIME, GETCOUNT, GETTIME }
    protected Text outKey = new Text();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        documentreader = new DocumentReaderContent();
        Document.setContentExtractor(new ContentExtractorPAN());
        index = new AnnChunk();
    }
    
    @Override
    public void map(Integer key, Pair value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        Datafile sourceDatafile = new Datafile(conf, value.sourcefile);
        ArrayList<Document> sourceDocuments = 
                documentreader.readDocuments(sourceDatafile);
        for (Document sourceDocument : sourceDocuments) {
            index.addDocument(sourceDocument);
        }

        // iterate over all suspicious documents
        Datafile queryDF = new Datafile(conf, value.queryfile);
        for (Document query : documentreader.iterableDocuments(queryDF)) {
            // retrieve the k most similar source documents from the index
            ArrayList<Passage> passages = index.getDocuments(query);

            // write the topk to the reducer
            outKey.set(query.getId());
            for (Passage passage : passages) {
                context.write(outKey, passage);
            }
        }
    }

    public void cleanup(Context context) {
        context.getCounter(COUNTER.ADDCOUNT).increment(Profiler.getCount("addDocument"));
        context.getCounter(COUNTER.ADDTIME).increment(Profiler.totalTimeMs("addDocument"));
        context.getCounter(COUNTER.GETCOUNT).increment(Profiler.getCount("getPassages"));
        context.getCounter(COUNTER.GETTIME).increment(Profiler.totalTimeMs("getPassages"));
    }
}
