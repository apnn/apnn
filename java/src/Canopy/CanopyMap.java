package Canopy;

import Canopy.AnnCanopy.Doc;
import SimilarityFile.IndexSimilarity;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTerms;
import TestGenericMR.SourceQueryPairInputFormat.Pair;
import TestGenericMR.TestGenericJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

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
public class CanopyMap extends Mapper<Integer, Pair, Text, SimilarityWritable> {

    public static final Log log = new Log(CanopyMap.class);
    protected Conf conf;
    protected DocumentReader documentreader;
    protected AnnCanopy index;
    protected int topk;
    SimilarityWritable similarityWritable = new SimilarityWritable();
    double T1;
    double T2;
    protected Text outKey = new Text();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        documentreader = getDocumentReader();
        topk = TestGenericJob.getTopK(conf);
        index = CanopyGenericJob.getAnnIndex(getComparator(), conf);
        T1 = index.T1;
        T2 = index.T2;
        Document.setSimilarityFunction(getSimilarityFunction());
    }

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTerms();
    }

    public SimilarityFunction getSimilarityFunction() {
        return TestGenericJob.getSimilarityFunction(conf);
    }

    public int getTopK() {
        return topk;
    }

    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    }

    @Override
    public void map(Integer key, Pair value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        if (value.queryfile == null) {
            ArrayList<Doc> docs = this.readDocsSource(value.sourcefile);
            for (int i = docs.size() - 1; i >= 0; i--) {
                Doc a = docs.get(i);
                for (int j = 0; j <= i; j++) {
                    Doc b = docs.get(j);
                    write(context, a, b);
                }
            }
        } else {
            ArrayList<Doc> docsa = this.readDocsSource(value.sourcefile);
            ArrayList<Doc> docsb;
            if (value.sourcefile.equals(value.queryfile))
                docsb = docsa;
            else
                docsb = this.readDocsQuery(value.queryfile);
            for (Doc docb : docsb) {
                similarityWritable.query = docb.getDocument().getId();
                outKey.set(similarityWritable.query);
                for (Doc doca : docsa) {
                    if (doca != docb) {
                        similarityWritable.indexSimilarity = index.fastDistance(doca, docb);
                        similarityWritable.measureSimilarity = index.distance(doca, docb);
                        similarityWritable.source = doca.document.getId();
                        context.write(outKey, similarityWritable);
                    }
                }
            }
        }
    }

    public void write(Context context, Doc a, Doc b) throws IOException, InterruptedException {
        if (a.document.getId().compareTo(b.document.getId()) > 0) {
            write(context, b, a);
        } else {
            similarityWritable.query = b.getDocument().getId();
            outKey.set("-" + a.getDocument().getId());
            similarityWritable.indexSimilarity = index.fastDistance(a, b);
            similarityWritable.measureSimilarity = index.distance(a, b);
            similarityWritable.source = null;
            context.write(outKey, similarityWritable);
        }
    }

    public ArrayList<Doc> readDocsSource(String file) throws IOException {
        Datafile df = new Datafile(conf, file);
        ArrayList<Doc> docs = new ArrayList();
        for (Document d : documentreader.readDocuments(df)) {
            docs.add(index.createDocumentSource(d));
        }
        return docs;
    }

    public ArrayList<Doc> readDocsQuery(String file) throws IOException {
        Datafile df = new Datafile(conf, file);
        ArrayList<Doc> docs = new ArrayList();
        for (Document d : documentreader.readDocuments(df)) {
            docs.add(index.createDocumentQuery(d));
        }
        return docs;
    }

    @Override
    public void cleanup(Context context) {
        TestGenericJob.addMeasuresCompared(context, Document.getSimilarityFunction().getComparisons());
        TestGenericJob.addGetDocumentsTime(context, AnnIndex.getGetDocumentsTime(), AnnIndex.getGetDocumentsCount());
        TestGenericJob.addGetDocCodepoints(context, index.countDocCodepoints);
        TestGenericJob.addGetDocComparedCodepoints(context, index.countComparedDocCodepoints);
        TestGenericJob.addFingerprintTime(context, AnnIndex.getGetFingerprintTime(), AnnIndex.getGetFingerprintCount());
        TestGenericJob.addSimilarityFunction(context, Document.getSimilarityFunction().getComparisonsTime(),
                Document.getSimilarityFunction().getComparisons());
    }

    public Configuration getConf() {
        return conf;
    }
}
