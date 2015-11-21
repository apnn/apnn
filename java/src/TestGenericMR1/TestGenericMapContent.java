package TestGenericMR1;

import TestGenericMR.*;

public class TestGenericMapContent extends TestGenericMap1 {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderContent();
    }
}
