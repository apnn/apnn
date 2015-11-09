package AnalyzeCosineSimilarity;

import SimilarityFunction.CosineSimilarityTFIDF;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.Document;
import TestGenericMR.StringPairInputFormat.Value;
import static TestGenericMR.TestGenericMap.iterableDocuments;
import static TestGenericMR.TestGenericMap.readDocuments;
import io.github.htools.collection.TopKMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import io.github.htools.type.TermVectorDouble;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
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
public class AnalyzeMap extends Mapper<Integer, Value, IntWritable, Result> {

    public static final Log log = new Log(AnalyzeMap.class);
    Conf conf;
    SimilarityFunction similarityFunction;

    protected IntWritable outKey = new IntWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        similarityFunction = new CosineSimilarityTFIDF(new Datafile(conf, "input/pan11/vocabulary"));
        Document.setTokenizer(conf);
    }
    
    @Override
    public void map(Integer key, Value value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        ArrayList<Document> sourceDocuments = readDocuments(conf, value.sourcefile, similarityFunction);

        // iterate over all suspicious documents
        for (Document suspiciousDocument : iterableDocuments(conf, value.suspiciousfile, similarityFunction)) {
            double maxsim = 0;
            TopKMap<Double, String> topTerms;
            Result outValue = new Result();
            
            // retrieve the k most similar source documents from the index
            for (Document s : sourceDocuments) {
                double similarity = similarityFunction.similarity(s, suspiciousDocument);
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
