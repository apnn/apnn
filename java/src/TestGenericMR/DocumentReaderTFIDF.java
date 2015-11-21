package TestGenericMR;

import TestGeneric.Document;
import io.github.htools.io.compressed.ArchiveEntry;
import java.io.IOException;

/**
 *
 * @author Jeroen
 */
public class DocumentReaderTFIDF extends DocumentReaderTerms {
    public Document readDocument(ArchiveEntry entry) throws IOException {
        return Document.readTFIDF(entry.getName(), entry.readAll());
    }
}
