package TestGeneric;

import SimilarityFunction.SimilarityFunction;
import io.github.htools.collection.ArrayMap;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.type.TermVector;
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
    private static Tokenizer tokenizer;
    public static ByteSearch wordsplitter = ByteSearch.create("\\S+");
    private static SimilarityFunction similarityFunction;
    static boolean stopwordsRemoved;

    public String docid;
    private ArrayList<String> terms;
    private TermVector model;
    private byte[] content;

    public Document(String docid, byte[] content) throws IOException {
        this.docid = docid;
        this.content = content;
    }

    public Document(String docid, Map<String, Double> tfidf) throws IOException {
        // note that tokenizer operates on the content in situ, therefore
        // content will be cleaned of any not text, lowercased, space will be 
        // trimmed an, however it will not be stemmed and stop words will not be removed.
        this.docid = docid;
        this.model = new TermVectorDouble(tfidf);
    }

    public Document(String docid, byte[] content, ArrayList<String> terms) throws IOException {
        this.docid = docid;
        setTerms(terms);
        // toBytes() strips the \0 bytes that were used to erase non-content.
        this.content = ByteTools.toBytes(content, 0, content.length);
    }

    public static Tokenizer getTokenizer() {
        if (tokenizer == null)
            tokenizer = new Tokenizer();
        return tokenizer;
    }
    
    public static void setTokenizer(Tokenizer tokenizer) {
        Document.tokenizer = tokenizer;
    }
    
    public static void setSimilarityFunction(SimilarityFunction function) {
        similarityFunction = function;
    }

    public static SimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }

    public static Document read(String id, ByteSearchSection section) throws IOException {
        byte[] readAll = section.toBytes();
        return new Document(id, readAll);
    }

    public static Document readContent(String id, byte[] content) throws IOException {
        return new Document(id, content);
    }

    public static Document readTerms(String id, byte[] content) throws IOException {
        return new Document(id, content, wordsplitter.extractAll(content));
    }

    static ByteSearch eol = ByteSearch.create("\\n");

    public static Document readTFIDF(String id, byte[] content) throws IOException {
        ByteSearch space = ByteSearch.WHITESPACE;
        ArrayMap<String, Double> tfidf = new ArrayMap();
        ArrayList<ByteSearchSection> split = eol.split(content);
        for (ByteSearchSection section : split) {
            ArrayList<ByteSearchSection> parts = space.split(section);
            tfidf.add(parts.get(0).toString(), Double.parseDouble(parts.get(1).toString()));
        }
        return new Document(id, tfidf);
    }

    public void setModel(TermVector v) {
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
    public TermVector getModel() {
        if (model == null) {
            model = new TermVectorEntropy(getTermsNoStopwords());
            similarityFunction.reweight(this);
        }
        return model;
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

    /**
     * @return an ArrayList of terms in the order they appear in the document.
     * Warning, using this will clear the getContent() representation.
     */
    public ArrayList<String> getTerms() {
        if (terms == null) {
            terms = tokenizer.tokenize(getContent());
        }
        return terms;
    }

    public ArrayList<String> getTermsNoStopwords() {
        ArrayList<String> result = new ArrayList();
        for (String term : getTerms()) {
            if (!tokenizer.isStopword(term)) {
                result.add(term);
            }
        }
        return result;
    }

    public byte[] getTokenizedContent() {
        if (terms == null) {
            getTerms();
        }
        return getContent();
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
