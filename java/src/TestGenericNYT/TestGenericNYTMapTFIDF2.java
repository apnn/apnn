package TestGenericNYT;

import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTFIDF2;

public class TestGenericNYTMapTFIDF2 extends TestGenericNYTMap {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF2();
    }
}
