package TestGenericRobust;

import TestGenericMR.*;

public class TestGenericFDMMapTFIDF extends TestGenericFDMMap {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF();
    }
}
