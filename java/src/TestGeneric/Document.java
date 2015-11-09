package TestGeneric;

import TestGenericMR.TestGenericJob;
import io.github.htools.collection.ArrayMap;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchPosition;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.type.TermVector;
import io.github.htools.type.TermVectorDouble;
import io.github.htools.type.TermVectorEntropy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

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
public class Document  {

    public static Log log = new Log(Document.class);
    public static Tokenizer tokenizer = new TokenizerRemoveStopwords();
    static boolean stopwordsRemoved;
    static ByteSearch docNumber = ByteSearch.create("\\d+");

    public int docid;
    private ArrayList<String> terms;
    private TermVector model;
    private byte[] content;

    public Document(int docid, byte[] content) throws IOException {
        // note that tokenizer operates on the content in situ, therefore
        // content will be cleaned of any not text, lowercased, space will be 
        // trimmed an, however it will not be stemmed and stop words will not be removed.
        this(docid, content, tokenizer.tokenize(content));
    }

    public Document(int docid, Map<String, Double> tfidf) throws IOException {
        // note that tokenizer operates on the content in situ, therefore
        // content will be cleaned of any not text, lowercased, space will be 
        // trimmed an, however it will not be stemmed and stop words will not be removed.
        this.docid = docid;
        this.model = new TermVectorDouble(tfidf);
    }

    public Document(int docid, byte[] content, ArrayList<String> terms) throws IOException {
        this.docid = docid;
        setTerms(terms);
        // toBytes() strips the \0 bytes that were used to erase non-content.
        this.content = ByteTools.toBytes(content, 0, content.length);
    }

    public static Document read(ArchiveEntry entry) throws IOException {
        byte[] readAll = entry.readAll();
        int docid = getDocNumber(entry.getName());
        return new Document(docid, readAll);
    }
    
    public static Document readTFIDF(ArchiveEntry entry) throws IOException {
        ByteSearch eol = ByteSearch.create("\\n");
        ByteSearch space = ByteSearch.WHITESPACE;
        ArrayMap<String, Double> tfidf = new ArrayMap();
        ArrayList<ByteSearchSection> split = eol.split(entry.readAll());
        for (ByteSearchSection section : split) {
            ArrayList<ByteSearchSection> parts = space.split(section);
            tfidf.add(parts.get(0).toString(), Double.parseDouble(parts.get(1).toString()));
        }
        int docid = getDocNumber(entry.getName());
        return new Document(docid, tfidf);
    }
    
    /**
     * If a tokenizer was configured, Document will use this tokenizer.
     *
     * @param conf
     */
    public static void setTokenizer(Configuration conf) {
        Class<? extends Tokenizer> tokenizerClass
                = TestGenericJob.getTokenizerClass(conf);
        if (tokenizerClass != null) {
            tokenizer = Tokenizer.get(tokenizerClass);
        }
    }

    public void setModel(TermVector v) {
        this.model = v;
    }
    
    /**
     *
     * @return The collection ID for the document, which is extracted as the
     * numeric part of the filename.
     */
    public int getId() {
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
            //log.info("getModel %s", terms);
            if (!stopwordsRemoved) {
                model = new TermVectorEntropy(tokenizer.removeStopwords(terms));
            } else {
                model = new TermVectorEntropy(terms);
            }
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
        return terms;
    }

    /**
     * @param docname filename of the document
     * @return the number form the filename, which indicates the documentID
     * @throws IOException
     */
    public static int getDocNumber(String docname) {
        return Integer.parseInt(docNumber.extract(docname));
    }

    @Override
    public int hashCode() {
        return MathTools.hashCode(docid);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Document) && (((Document) o).docid == docid);
    }

    public int compareTo(Document o) {
        return docid - o.docid;
    }
}
