package VocabularyNYT;

import TestGeneric.ContentExtractorNYT;
import TestGeneric.Document;
import io.github.htools.lib.Log;

public class VocabularyMap extends VocabularyPAN.VocabularyMap {

    public static final Log log = new Log(VocabularyMap.class);

    @Override
    public void setup(Context context) {
        super.setup(context);
        // use different content extractor
        Document.setContentExtractor(new ContentExtractorNYT());
    }
}
