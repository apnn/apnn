package TestGeneric;

import SimilarityFile.SimilarityWritable;

/**
 *
 * @author Jeroen
 */
public class Candidate extends SimilarityWritable {
    public Document document;

    public Candidate() {
    }

    public Candidate(Document d, double indexsimilarity) {
        this.document = d;
        this.source = d.docid;
        this.indexSimilarity = indexsimilarity;
    }

    public Candidate clone() {
        Candidate clone = new Candidate();
        clone.query = query;
        clone.source = source;
        clone.indexSimilarity = indexSimilarity;
        clone.measureSimilarity = measureSimilarity;
        return clone;
    }
}
