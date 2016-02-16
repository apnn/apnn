package TestGeneric;

import SimilarityFunction.SimilarityFunction;
import io.github.htools.collection.ArrayMap;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.type.TermVectorDouble;
import io.github.htools.type.TermVectorEntropy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * A Document in the PAN11 collection, that is stored in a ArchiveFile (e.g.
 * tar) as a separate file, with a filename that contains the numeric document
 * ID, and the raw contents is a flat text file. A Document can be constructed
 * straight from the ArchiveEntry of an ArchiveFile, and tokenizes the content
 * by lowercasing and removing stop words.
 *
 * Document can provide the content in three possible ways: as a byte[] array,
 * an ArrayList of terms in the order in which they appear in the document or as
 * a map containing the frequency per term (not preserving term order). Warning!
 * we assume you only use one representation at a time, when you call
 * getContent(), getTerms() or getModel() the other representations are removed
 * to release memory.
 *
 * @author jeroen
 */
public class Document {

    public static Log log = new Log(Document.class);
    protected static ContentExtractor extractor;
    private static SimilarityFunction similarityFunction;
    static boolean stopwordsRemoved;

    public String docid;
    private ArrayList<String> terms;
    private TermVectorDouble model;
    private TermVectorEntropy emodel;
    private byte[] content;
    private byte[] originalcontent;

    public Document(String docid, byte[] content) throws IOException {
        this.docid = docid;
        this.content = content;
    }

    public Document(String docid, byte[] originalcontent, byte[] content) throws IOException {
        //log.info("new Document %s %s", docid, originalcontent);
        this.docid = docid;
        this.originalcontent = originalcontent;
        this.content = content;
    }

    public Document(Document doc) {
        this.docid = doc.docid;
        this.content = doc.content;
        this.originalcontent = doc.originalcontent;
        this.terms = doc.terms;
        this.model = doc.model;
    }

    public Document(String docid, Map<String, Double> tfidf) throws IOException {
        // note that tokenizer operates on the content in situ, therefore
        // content will be cleaned of any not text, lowercased, space will be 
        // trimmed an, however it will not be stemmed and stop words will not be removed.
        this.docid = docid;
        this.model = new TermVectorDouble(tfidf);
    }

    public static void setContentExtractor(ContentExtractor tokenizer) {
        Document.extractor = tokenizer;
    }

    public static void setSimilarityFunction(SimilarityFunction function) {
        similarityFunction = function;
    }

    public static SimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }

    public static ContentExtractor getContentExtractor() {
        return extractor;
    }

    public static Document readContent(String id, byte[] content) throws IOException {
        byte[] contentCopy = ArrayTools.clone(content);
        return new Document(id, contentCopy, extractor.extractContent(content));
    }

    public static Document readTerms(String id, byte[] content) throws IOException {
        return new Document(id, content);
    }

    static ByteSearch eol = ByteSearch.create("\\n");

    public static Document readTFIDF(String id, byte[] content) throws IOException {
        ByteSearch space = ByteSearch.WHITESPACE;
        ArrayMap<String, Double> tfidf = new ArrayMap();
        ArrayList<ByteSearchSection> split = eol.split(content);
        for (ByteSearchSection section : split) {
            ArrayList<ByteSearchSection> parts = space.split(section);
            if (parts.size() < 2)
                log.fatal("readTFIDF %s", ByteTools.toString(content));
            tfidf.add(parts.get(0).toString(), Double.parseDouble(parts.get(1).toString()));
        }
        return new Document(id, tfidf);
    }

    public static Document readTFIDF2(String id, byte[] content) throws IOException {
        ByteSearch space = ByteSearch.WHITESPACE;
        ArrayMap<String, Double> tfidf = new ArrayMap();
        ArrayList<ByteSearchSection> split = eol.split(content);
        double magnitude = 0;
        for (ByteSearchSection section : split) {
            ArrayList<ByteSearchSection> parts = space.split(section);
            if (parts.size() < 2)
                log.fatal("readTFIDF %s", ByteTools.toString(content));
            double freq = Double.parseDouble(parts.get(1).toString());
            tfidf.add(parts.get(0).toString(), freq);
            magnitude += freq * freq;
        }
        magnitude = Math.sqrt(magnitude);
        for (Map.Entry<String, Double> entry : tfidf) {
            entry.setValue(entry.getValue() / magnitude);
        }
        return new Document(id, tfidf);
    }

    public void setModel(TermVectorDouble v) {
        this.model = v;
    }

    /**
     * Release memory occupied by content.
     */
    public void clearContent() {
        content = null;
    }

    /**
     * Release memory occupied by the tokenized terms, this will still keep the
     * model to allow cossim computations.
     */
    public void clearTerms() {
        terms = null;
    }

    /**
     *
     * @return The collection ID for the document, which is extracted as the
     * numeric part of the filename.
     */
    public String getId() {
        return docid;
    }

    protected void setTerms(ArrayList<String> terms) {
        this.terms = terms;
    }

    /**
     * @return A map of term-frequencies, that can be used to estimate term
     * independent similarity between documents. Releases the ordered list of
     * tokenized terms and the raw content from memory, so after calling this
     * getModel() and getTerms() can no longer be used.
     */
    public TermVectorDouble getModel() {
        if (model == null) {
            model = new TermVectorDouble(getTerms());
            if (similarityFunction != null) {
                similarityFunction.reweight(this);
            }
        }
        return model;
    }

    public TermVectorEntropy getEModel() {
        if (emodel == null) {
            emodel = new TermVectorEntropy(getTerms());
        }
        return emodel;
    }

    /**
     * @return a byte array of the semi-processed content in the document. Non-
     * text has been removed, contiguous white spaces are compressed to a single
     * space, the text is lowercased, but stop words are not removed and no
     * stemming is done. When you call this it will release the tokenized terms
     * to release memory to free memory, so getTerms() and getModel() can not be
     * used after this call.
     */
    public byte[] getContent() {
        return content;
    }

    public byte[] getOriginalContent() {
        return originalcontent;
    }

    /**
     * @return an ArrayList of terms in the order they appear in the document.
     * Warning, using this will clear the getContent() representation.
     */
    public ArrayList<String> getTerms() {
        if (terms == null) {
            terms = new ArrayList();
            for (String term : getTermsStopwords()) {
                if (!extractor.isStopword(term)) {
                    terms.add(term);
                }
            }
        }
        return terms;
    }

    public ArrayList<String> getTermsStopwords() {
        ArrayList<String> terms = new ArrayList();
        for (String term : extractor.getTokens(getContent())) {
            term = term.toLowerCase();
            terms.add(term);
        }
        return terms;
    }

    @Override
    public int hashCode() {
        return MathTools.hashCode(docid.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Document) && (((Document) o).docid.equals(docid));
    }

    public int compareTo(Document o) {
        return docid.compareTo(o.docid);
    }

    public double similarity(Document o) {
        return similarityFunction.similarity(this, o);
    }
}
