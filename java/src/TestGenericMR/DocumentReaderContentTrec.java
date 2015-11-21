package TestGenericMR;

import TestGeneric.Document;
import TestGeneric.TokenizerTrec;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.search.ByteSection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author Jeroen
 */
public class DocumentReaderContentTrec implements DocumentReader {

    public static Log log = new Log(DocumentReaderContentTrec.class);
    static ByteSection DOC = ByteSection.create("<doc>", "</doc>");
    static ByteSection DOCNO = ByteSection.create("<docno>", "</docno>");

    public DocumentReaderContentTrec() {
       Document.setTokenizer( new TokenizerTrec() ); 
    }
    
    /**
     * @param documentFilename
     * @return an ArrayList of Documents read from an ArchiveFile on HDFS with
     * the name documentFilename
     * @throws IOException
     */
    public ArrayList<Document> readDocuments(Datafile file) throws IOException {
        ArrayList<Document> documents = new ArrayList();
        for (Document document : iterableDocuments(file)) {
            // extract the docid from the filename (in the tar-file)
            // add to the map of documents
            documents.add(document);
        }
        return documents;
    }

    /**
     * An Iterable over the documents in an archiveFile on HDFS with the name
     * documentFilename
     *
     * @param documentFilename
     * @return
     * @throws IOException
     */
    public Iterable<Document> iterableDocuments(Datafile file) throws IOException {
        return new DocumentIterator(file);
    }

    public static Document readDocument(ByteSearchSection section) throws IOException {
        ByteSearchSection docidsection = DOCNO.findPos(section);
        String docid = docidsection.toTrimmedString();
        docidsection.erase();
        Document document = Document.read(docid, section);
        return document;
    }

    static class DocumentIterator implements Iterable<Document>, Iterator<Document> {

        Datafile file;
        long end;
        Document next;

        DocumentIterator(Datafile file) {
            log.info("DocumentIterator %s %d %d", file.getCanonicalPath(), file.getOffset(), file.getCeiling());
            this.file = file;
            end = file.getCeiling();
            file.setCeiling(Long.MAX_VALUE);
            file.setBufferSize(1000000);
            file.openRead();
            next();
        }

        @Override
        public Iterator<Document> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Document next() {
            Document result = next;
            next = null;
            try {
                ByteSearchSection readSection = file.readSection(DOC);
                if (readSection.found() && readSection.start + file.rwbuffer.offset < end ) {
                    Document document = readDocument(readSection);
                    next = document;
                } else {
                    file.closeRead();
                }
            } catch (IOException ex) {
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

}
