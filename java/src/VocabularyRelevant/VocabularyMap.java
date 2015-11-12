package VocabularyRelevant;

import Vocabulary.*;
import TestGeneric.Tokenizer;
import TestGeneric.TokenizerRemoveStopwords;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.type.TermVectorInt;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class VocabularyMap extends Mapper<Object, String, Text, IntWritable> {

    public static final Log log = new Log(VocabularyMap.class);
    public static final String COLLECTIONSIZE = "#DOCS";
    Configuration conf;
    Tokenizer tokenizer = new TokenizerRemoveStopwords();
    Text outkey = new Text();
    Text outdf = new Text(COLLECTIONSIZE);
    IntWritable outvalue = new IntWritable();
    IntWritable zerovalue = new IntWritable(0);

    public void setup(Context context) {
        conf = ContextTools.getConfiguration(context);
    }

    @Override
    public void map(Object key, String value, Context context) throws IOException, InterruptedException {
        ArchiveFile archiveFile = ArchiveFile.getReader(conf, value);
        boolean sourceFile = value.contains("source");
        for (ArchiveEntry archiveEntry : (Iterable<ArchiveEntry>) archiveFile) {
            byte[] readAll = archiveEntry.readAll();

            ArrayList<String> terms = tokenizer.tokenize(readAll);

            HashSet<String> model = new HashSet(terms);
            for (String term : model) {
                outkey.set(term);
                if (sourceFile) {
                    outvalue.set(1);
                    context.write(outkey, outvalue);
                } else {
                    context.write(outkey, zerovalue);
                }
            }
        }
    }
}
