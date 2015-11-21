package AnalyzeCosineSimilarity;

import SimilarityFunction.CosineSimilarityTFIDF;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTerms;
import TestGenericMR.SourceQueryPairInputFormat.Pair;
import io.github.htools.collection.TopKMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Every map receives one Value as input, that contains a unique combination of
 * a filename with a block of suspicious documents and a filename with a block
 * of source documents. The default operation is to do a brute force comparison
 * by reading all source documents into memory, and
 * then the suspicious documents are inspected one-at-a-time for matches in the
 * index. The k-most similar source documents are send to the reducer. This
 * generic mapper can be configured by setting the index, similarity function
 * and k in the job, or overridden to add functionality.
 *
 * @author jeroen
 */
public class AnalyzeMap extends Mapper<Integer, Pair, Text, Result> {

    public static final Log log = new Log(AnalyzeMap.class);
    Conf conf;
    DocumentReader documentreader;

    protected Text outKey = new Text();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        documentreader = new DocumentReaderTerms();
        Document.setSimilarityFunction(new CosineSimilarityTFIDF(new Datafile(conf, "input/pan11/vocabulary")));
    }
    
    @Override
    public void map(Integer key, Pair value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        Datafile sourceDatafile = new Datafile(conf, value.sourcefile);
        ArrayList<Document> sourceDocuments = 
                documentreader.readDocuments(sourceDatafile);

        // iterate over all suspicious documents
        Datafile suspDatafile = new Datafile(conf, value.queryfile);
        for (Document suspiciousDocument : 
                documentreader.iterableDocuments(suspDatafile)) {
            double maxsim = 0;
            TopKMap<Double, String> topTerms;
            Result outValue = new Result();
            
            // retrieve the k most similar source documents from the index
            for (Document s : sourceDocuments) {
                double similarity = s.similarity(suspiciousDocument);
                if (similarity > maxsim) {
                    maxsim = similarity;
                    topTerms = new TopKMap(5);
                    TermVectorDouble model1 = (TermVectorDouble)suspiciousDocument.getModel();
                    TermVectorDouble model2 = (TermVectorDouble)s.getModel();
                    for (Map.Entry<String, Double> entry : model1.entrySet()) {
                        Double v2 = model2.get(entry.getKey());
                        if (v2 != null) {
                            topTerms.add(entry.getValue() * v2, entry.getKey());
                        }
                    }
                    double magnitude = s.getModel().magnitude() * suspiciousDocument.getModel().magnitude();
                    outValue.set(suspiciousDocument.docid, s.docid, similarity, magnitude, topTerms);
                }
            }
            // write the topk to the reducer
            outKey.set(suspiciousDocument.getId());
            context.write(outKey, outValue);
        }
    }
}
