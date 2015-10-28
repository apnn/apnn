package Vocabulary;

import io.github.htools.collection.ArrayMap3;
import io.github.htools.lib.Log;
import java.io.IOException;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.type.Tuple2;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Clusters the titles of one single day, starting with the clustering results
 * at the end of yesterday,
 *
 * @author jeroen
 */
public class VocabularyReduce extends Reducer<Text, IntWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(VocabularyReduce.class);
    ArrayMap3<Integer, String, Integer> termmap = new ArrayMap3();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int documentFrequency = 0;
        int termFrequency = 0;
        for (IntWritable entry : values) {
            documentFrequency++;
            termFrequency += entry.get();
        }
        termmap.add(termFrequency, key.toString(), documentFrequency);
    }

    @Override
    public void cleanup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        VocabularyFile vocabularyFile = new VocabularyFile(conf.getHDFSFile("output"));
        vocabularyFile.setBufferSize(1000000);
        vocabularyFile.openWrite();
        VocabularyWritable w = new VocabularyWritable();
        termmap.descending();
        for (Map.Entry<Integer, Tuple2<String, Integer>> entry : termmap) {
            w.term = entry.getValue().key;
            w.termFrequency = entry.getKey();
            w.documentFrequency = entry.getValue().getValue();
            w.write(vocabularyFile);
        }
        vocabularyFile.closeWrite();
    }
}
