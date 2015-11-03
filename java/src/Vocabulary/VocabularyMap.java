package Vocabulary;

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
            //if (archiveEntry.getName().equals("source-document06619.txt")) {
                //byte[] copy = ArrayTools.clone(readAll);
                ArrayList<String> terms = tokenizer.tokenize(readAll);
                //log.info("%s %s", archiveEntry.getName(), terms);

//                Datafile df = new Datafile(conf, "aap1");
//                df.openWrite();
//                df.printf("%s\n\n", ByteTools.toString(readAll));
//                for (String t : terms) {
//                    df.printf("%s\n", t);
//                }
//                df.closeWrite();
//                log.crash();

                TermVectorInt model = new TermVectorInt(terms);
                for (Map.Entry<String, Integer> entry : model.entrySet()) {
                    outkey.set(entry.getKey());
                    if (sourceFile) {
                        outvalue.set(entry.getValue());
                        context.write(outkey, outvalue);
                    } else {
                        context.write(outkey, zerovalue);
                    }
                }
                if (sourceFile) {
                    context.write(outdf, zerovalue);
                }
            //}
        }
    }
}
