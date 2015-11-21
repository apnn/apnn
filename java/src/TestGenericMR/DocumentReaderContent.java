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
    
    public String getDocID(String filename) {
        return Integer.toString(Integer.parseInt(filename));
    }
}
