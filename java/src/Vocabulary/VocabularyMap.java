package Vocabulary;

import TestGeneric.Tokenizer;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.extract.Content;
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
    Configuration conf;
    Tokenizer tokenizer = new Tokenizer();
    Text outkey = new Text();
    IntWritable outvalue = new IntWritable();

    public void setup(Context context) {
        conf = ContextTools.getConfiguration(context);
    }

    @Override
    public void map(Object key, String value, Context context) throws IOException, InterruptedException {
        ArchiveFile archiveFile = ArchiveFile.getReader(conf, value);
        for (ArchiveEntry archiveEntry : (Iterable<ArchiveEntry>) archiveFile) {
            ArrayList<String> terms = tokenizer.tokenize(archiveEntry.readAll());
            TermVectorInt model = new TermVectorInt(terms);
            for (Map.Entry<String, Integer> entry : model.entrySet()) {
                outkey.set(entry.getKey());
                outvalue.set(entry.getValue());
                context.write(outkey, outvalue);
            }
        }
    }
}
