package TestGenericMR1;

import TestGenericMR.*;

public class TestGenericMapTFIDF extends TestGenericMap1 {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF();
    }
}
