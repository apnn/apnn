package TestGenericMR;

public class TestGenericMapTFIDF extends TestGenericMap {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF();
    }
}
