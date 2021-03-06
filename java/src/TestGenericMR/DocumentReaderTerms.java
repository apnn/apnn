package TestGenericMR;

import TestGeneric.Document;
import io.github.htools.io.Datafile;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.lib.Log;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author Jeroen
 */
public class DocumentReaderTerms implements DocumentReader {

    public static Log log = new Log(DocumentReaderTerms.class);
    public static FileSystem fs;

    /**
     * @param file
     * @return an ArrayList of Documents read from an ArchiveFile on HDFS with
     * the name documentFilename
     * @throws IOException
     */
    public ArrayList<Document> readDocuments(Datafile file) throws IOException {
        fs = file.getFileSystem();
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
     * @param file
     * @return
     * @throws IOException
     */
    public Iterable<Document> iterableDocuments(Datafile file) {
        fs = file.getFileSystem();
        file.setBufferSize(1000000);
        ArchiveFile archiveFile = ArchiveFile.getReader(file);
        return new DocumentIterator(archiveFile);
    }

    public Document readDocument(ArchiveEntry entry) throws IOException {
        return Document.readTerms(entry.getName(), entry.readAll());
    }

    static int i = 0;
    class DocumentIterator implements Iterable<Document>, Iterator<Document> {

        Iterator<ArchiveEntry> iterator;

        DocumentIterator(ArchiveFile file) {
            iterator = file;
        }

        @Override
        public Iterator<Document> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Document next() {
            try {
                ArchiveEntry next = iterator.next();
                if (next != null) {
                    Document document = readDocument(next);
                    return document;
                }
            } catch (IOException ex) {
                log.fatalexception(ex, "next readDocument");
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

}
