package Canopy;

import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTFIDF2;

public class CanopyMapTFIDF2 extends CanopyMap {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF2();
    }
}
