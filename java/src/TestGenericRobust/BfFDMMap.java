package TestGenericRobust;

import TestGenericMR.*;
import TestGeneric.AnnIndex;
import TestGeneric.ContentExtractor;
import TestGeneric.ContentExtractorRobust;
import TestGeneric.Document;
import static TestGenericRobust.BfFDMJob.setQueries;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Every map receives one Value as input, that contains a unique combination of
 * a filename with a block of suspicious documents and a filename with a block
 * of source documents. The default operation is to do a brute force comparison
 * by reading all source documents into memory, and then the suspicious
 * documents are inspected one-at-a-time for matches in the index. The k-most
 * similar source documents are send to the reducer. This generic mapper can be
 * configured by setting the index, similarity function and k in the job, or
 * overridden to add functionality.
 *
 * @author jeroen
 */
public class BfFDMMap extends Mapper<String, String, Text, FDMDoc> {

    public static final Log log = new Log(BfFDMMap.class);
    protected Conf conf;
    protected DocumentReader documentreader;
    protected AnnIndex index;
    protected int topk;
    protected ArrayList<FDMQuery> queries;

    protected Text outKey = new Text();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        documentreader = getDocumentReader();
        topk = BfFDMJob.getTopK(conf);
        Document.setContentExtractor(new ContentExtractorRobust());
        queries = setQueries(conf, documentreader);
    }

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTerms();
    }

    @Override
    public void map(String key, String sourceFilename, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        Datafile sourceDatafile = new Datafile(conf, sourceFilename);
        for (Document sourceDocument : documentreader.iterableDocuments(sourceDatafile)) {
            log.info("source %s %s", sourceDocument.getId(), sourceDocument.getTerms());
            for (int i = 0; i < queries.size(); i++) {
                FDMQuery q = queries.get(i);
                //if (q.id.equals("690")) {
                    //log.info("query %s %s", q.id, q.termlist);
                    FDMDoc doc = new FDMDoc(sourceDocument, q);
                    if (doc.independent.size() > 0) {
                        //log.info("doc %s %s", doc.id, doc.independent);
                        outKey.set(q.id);
                        context.write(outKey, doc);
                    }
                //}
            }
        }
    }
}
