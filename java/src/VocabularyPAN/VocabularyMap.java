package VocabularyPAN;

import TestGeneric.ContentExtractorPAN;
import TestGeneric.Document;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTerms;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class VocabularyMap extends Mapper<Object, Datafile, Text, LongWritable> {

    public static final Log log = new Log(VocabularyMap.class);
    // #DOCS is used to compute #documents and #words in the entire collection
    public static final String COLLECTIONSIZE = "#DOCS";
    Configuration conf;
    DocumentReader documentreader;
    Text outkey = new Text();
    Text outdf = new Text(COLLECTIONSIZE);
    LongWritable outvalue = new LongWritable();
    LongWritable zerovalue = new LongWritable(0);

    @Override
    public void setup(Context context) {
        conf = ContextTools.getConfiguration(context);
        documentreader = new DocumentReaderTerms();
        Document.setContentExtractor(new ContentExtractorPAN());
    }

    @Override
    public void map(Object key, Datafile file, Context context) throws IOException, InterruptedException {
        boolean countFrequencies = !file.getName().contains("suspicious") && !file.getName().contains("quer");
        for (Document document : documentreader.iterableDocuments(file)) {
            ArrayList<String> terms = document.getTermsStopwords();
            TermVectorInt model = new TermVectorInt(terms);
            for (Map.Entry<String, Integer> entry : model.entrySet()) {
                outkey.set(entry.getKey());
                if (countFrequencies) {
                    outvalue.set(entry.getValue());
                    context.write(outkey, outvalue);
                } else {
                    context.write(outkey, zerovalue);
                }
            }
            if (countFrequencies) {
                outvalue.set(model.total());
                context.write(outdf, outvalue);
            }
        }
    }
}
