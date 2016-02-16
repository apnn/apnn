package Canopy;

import TestGenericMR.*;

public class CanopyMapTFIDF extends CanopyMap {

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF();
    }
}
