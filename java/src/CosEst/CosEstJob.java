package CosEst;

import TestGeneric.Tokenizer;
import TestGenericMR.TestGenericJobIE;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import org.apache.hadoop.conf.Configuration;

/**
 * Basis for this method is to use only the top-k tfidf terms per document. For 
 * the index, these are stored in an inverted list <term, list<document, n-tfidf>>,
 * where n-tfidf is the tf * idf / ||D||, or the tf-idf for the term divided by 
 * the magnitude of the document vector. For suspicious documents, the postings 
 * lists for its top-k tfidf terms are traversed, per source document accumulating 
 * the dot-product between its n-tfidf and the n-tfidf of the suspicious document.
 * Thus per document we obtain a lower bound estimation of the cosine based on the
 * top-k terms of each document.
 *
 * parameters:
 * vocabulary=vocfile : points to the vocabulary file
 * termssize=k : top-k terms to use for each document
 * 
 * @author Jeroen
 */
public class CosEstJob {

    private static final Log log = new Log(CosEstJob.class);
    public static final String TERMSSIZE = "termssize";

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath suspiciouspath output");

        TestGenericJobIE job = new TestGenericJobIE(conf,
                conf.get("sourcepath"),
                conf.get("suspiciouspath"),
                conf.get("output")
        );

        job.setAnnIndex(AnnCosEst.class);

        // don't remove stopwords when creating shingles
        //job.setTokenizer(Tokenizer.class);

        job.waitForCompletion(true);
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
