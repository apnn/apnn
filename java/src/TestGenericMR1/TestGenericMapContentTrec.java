package TestGenericMR1;

import TestGenericMR.*;

public class TestGenericMapContentTrec extends TestGenericMap1 {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderContentTrec();
    }
}
