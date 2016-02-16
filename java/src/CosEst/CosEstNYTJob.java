package CosEst;

import TestGenericMR.TestGenericJob;
import TestGenericNYT.TestGenericNYTJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;

/**
 * Basis for this method is to use only the top-k tfidf terms per document. For
 * the index, these are stored in an inverted list <term,
 * list<document, n-tfidf>>, where n-tfidf is the tf * idf / ||D||, or the
 * tf-idf for the term divided by the magnitude of the document vector. For
 * suspicious documents, the postings lists for its top-k tfidf terms are
 * traversed, per source document accumulating the dot-product between its
 * n-tfidf and the n-tfidf of the suspicious document. Thus per document we
 * obtain a lower bound estimation of the cosine based on the top-k terms of
 * each document.
 *
 * parameters: vocabulary=vocfile : points to the vocabulary file termssize=k :
 * top-k terms to use for each document
 *
 * @author Jeroen
 */
public class CosEstNYTJob {

    private static final Log log = new Log(CosEstNYTJob.class);
    public static final String TERMSSIZE = "termssize";

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath output vocabulary -t [termssize] -k [topk] --noninteractive");
        TestGenericJob.setAnnIndex(conf, AnnCosEst.class);

        TestGenericNYTJob job = new TestGenericNYTJob(conf,
                conf.get("sourcepath"),
                conf.get("output"),
                conf.get("vocabulary"));
        
        log.info("%s", job.getMapperClass().getCanonicalName());
        HDFSPath out = conf.getHDFSPath("output");
        out.trash();
        
        job.useDocumentNYTTFIDF2();
        // don't remove stopwords when creating shingles
        //job.setTokenizer(Tokenizer.class);

        job.submitJob();
        job.mergeResults();
    }

    /**
     * @param conf
     * @return the consecutive number of characters to use as a shingle
     * (default=9)
     * @throws ClassNotFoundException
     */
    public static int getTermsSize(Configuration conf) {
        return conf.getInt(TERMSSIZE, 100);
    }
}
