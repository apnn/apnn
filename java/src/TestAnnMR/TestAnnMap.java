package TestAnnMR;

import TestGeneric.AnnIndex;
import TestGeneric.Document;
import TestGenericMR.StringPairInputFormat.Value;
import TestGenericMR.TestGenericMap;
import io.github.htools.collection.TopKMap;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Every map receives one Value as input, that contains a unique combination of
 * a filename with a block of suspicious documents and a filename with a block
 * of source documents. The source documents are read and added to an index, and
 * then the suspicious documents are inspected one-at-a-time for matches in the
 * index. The k-most similar source documents are send to the reducer. This
 * generic mapper can be configured by setting the index, similarity function
 * and k in the job, or overridden to add functionality.
 *
 * @author jeroen
 */
public class TestAnnMap extends TestGenericMap {

    public static final Log log = new Log(TestAnnMap.class);
    AnnIndex index;

    @Override
    public void setup(Context context) throws IOException {
        super.setup(context);
        index = TestAnnJob.getAnnIndex(getSimilarityFunction(), getConf());
    }

    @Override
    public void map(Integer key, Value value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        ArrayList<Document> sourceDocuments = readDocuments(value.sourcefile);
        for (Document sourceDocument : sourceDocuments) {
            index.add(sourceDocument);
        }

        // iterate over all suspicious documents
        for (Document suspiciousDocument : iterableDocuments(value.suspiciousfile)) {
            // retrieve the k most similar source documents from the index
            TopKMap<Double, Document> topKSourceDocuments = index.getNNs(suspiciousDocument, getTopK());

            // write the topk to the reducer
            outKey.set(suspiciousDocument.getId());
            outValue.id = suspiciousDocument.getId();
            for (Map.Entry<Double, Document> entry : topKSourceDocuments) {
                double similarity = entry.getKey();
                Document sourceDocument = entry.getValue();
                outValue.score = similarity;
                outValue.source = sourceDocument.getId();

                // send to reducer
                context.write(outKey, outValue);
            }
        }
    }
}
