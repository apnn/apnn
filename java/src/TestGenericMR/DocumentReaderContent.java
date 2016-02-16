package TestGenericMR;

import TestGeneric.Document;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.search.ByteSearch;
import java.io.IOException;

/**
 *
 * @author Jeroen
 */
public class DocumentReaderContent extends DocumentReaderTerms {
    ByteSearch number = ByteSearch.create("\\d+");
    public Document readDocument(ArchiveEntry entry) throws IOException {
        return Document.readContent(getDocID(entry.getName()), entry.readAll());
    }

    /**
     * @param filename
     * @return the document ID which must be used as the filename.
     */
    public String getDocID(String filename) {
        filename = filename.substring(filename.lastIndexOf('/') + 1);
        return Integer.toString(Integer.parseInt(number.extract(filename)));
    }
}
