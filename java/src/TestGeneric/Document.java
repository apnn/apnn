package TestGeneric;

import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.hadoop.io.buffered.Writable;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.lib.MathTools;
import io.github.htools.search.ByteSearch;
import io.github.htools.type.TermVectorInt;
import java.io.IOException;
import java.util.Collection;

/**
 * A Document in the PAN11 collection, that is stored in a ArchiveFile (e.g. tar)
 * as a separate file, with a filename that contains the numeric document ID, and
 * the raw contents is a flat text file. A Document can be constructed straight from
 * the ArchiveEntry of an ArchiveFile, and tokenizes the content by lowercasing
 * and removing stop words. 
 * 
 * @author jeroen
 */
public class Document extends Writable {
    static DefaultTokenizer tokenizer = new DefaultTokenizer().removeStopWords();
    static ByteSearch docNumber = ByteSearch.create("\\d+");

    int docid;
    TermVectorInt model = new TermVectorInt();
    
    public Document(ArchiveEntry entry) throws IOException {
        this(getDocNumber(entry.getName()), entry.readAll());
    }
    
    public Document(int docid, byte[] content) throws IOException {
        this(docid, tokenizer.tokenize(content));
    }
    
    public Document(int docid, Collection<String> terms) throws IOException {
        this.docid = docid;
        addTerms(terms);
    }
    
    /**
     * 
     * @return The collection ID for the document, which is extracted as the
     * numeric part of the filename.
     */
    public int getId() {
        return docid;
    }
    
    protected void addTerms(Collection<String> terms) {
        model.addAll(terms);
    }
    
    /**
     * @return A map of term-frequencies, that can be used to estimate term independent
     * similarity between documents.
     */
    public TermVectorInt getModel() {
        return model;
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
        return (o instanceof Document) && (((Document)o).docid == docid);
    }
    
    public int compareTo(Document o) {
        return docid - o.docid;
    }    
    
    /**
     * Used for serialization with Hadoop
     * @param writer 
     */
    @Override
    public void write(BufferDelayedWriter writer) {
       writer.write(getId());
       getModel().write(writer);
    }

    /**
     * Used for serialization with Hadoop
     * @param reader 
     */
    @Override
    public void readFields(BufferReaderWriter reader) {
       docid = reader.readInt();
       if (model.size() > 0)
           model = new TermVectorInt();
       model.read(reader);
    }
    
}
