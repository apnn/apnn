package TestGenericMR;

public class TestGenericMapTFIDF extends TestGenericMapTerms {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF();
    }
}
