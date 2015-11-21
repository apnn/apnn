package Vocabulary;

import TestGeneric.Document;
import TestGeneric.Tokenizer;
import TestGeneric.TokenizerRemoveStopwords;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTerms;
import TestGenericMR.TestGenericJob;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.type.TermVectorInt;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class VocabularyMap extends Mapper<Object, Datafile, Text, IntWritable> {

    public static final Log log = new Log(VocabularyMap.class);
    public static final String COLLECTIONSIZE = "#DOCS";
    Configuration conf;
    DocumentReader documentreader;
    Text outkey = new Text();
    Text outdf = new Text(COLLECTIONSIZE);
    IntWritable outvalue = new IntWritable();
    IntWritable zerovalue = new IntWritable(0);

    public void setup(Context context) {
        conf = ContextTools.getConfiguration(context);
        documentreader = new DocumentReaderTerms();
    }

    @Override
    public void map(Object key, Datafile file, Context context) throws IOException, InterruptedException {
        boolean countFrequencies = !file.getName().contains("suspicious") && !file.getName().contains("quer");
        for (Document document : documentreader.iterableDocuments(file)) {
            //if (archiveEntry.getName().equals("source-document06619.txt")) {
            //byte[] copy = ArrayTools.clone(readAll);
            ArrayList<String> terms = document.getTerms();

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
                context.write(outdf, zerovalue);
            }
            //}
        }
    }
}
