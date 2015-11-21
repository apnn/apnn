package TestGenericMR;

public class TestGenericMapContentTrec extends TestGenericMap {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderContentTrec();
    }
}
