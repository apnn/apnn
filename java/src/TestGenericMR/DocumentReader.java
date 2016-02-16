package TestGenericMR;

import TestGeneric.Document;
import io.github.htools.io.Datafile;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author Jeroen
 */
public interface DocumentReader {
    
    /**
     * @param file
     * @return an ArrayList of Documents read from an ArchiveFile on HDFS with
     * the name documentFilename
     * @throws IOException
     */
    public ArrayList<Document> readDocuments(Datafile file) throws IOException;

    /**
     * An Iterable over the documents in an archiveFile on HDFS with the name
     * documentFilename
     *
     * @param file
     * @return
     * @throws IOException
     */
    public Iterable<Document> iterableDocuments(Datafile file) throws IOException;
}
