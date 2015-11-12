package VocabularyRelevant;

import Vocabulary.*;
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
    //ArrayList<String> termmap = new ArrayMap3();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        boolean inSource = false;
        boolean inSusp = false;
            for (IntWritable entry : values) {
                if (entry.get() > 0) { // suspicious documents have freq=0
                    inSource = true;
                } else {
                    inSusp = true;
                }
            }
            //if (inSource && inSusp)
        //termmap.add(documentFrequency, key.toString(), termFrequency);
    }

    @Override
    public void cleanup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        VocabularyFile vocabularyFile = new VocabularyFile(conf.getHDFSFile("output"));
        vocabularyFile.setBufferSize(1000000);
        vocabularyFile.openWrite();
        VocabularyWritable w = new VocabularyWritable();
        //.descending();
//        for (Map.Entry<Integer, Tuple2<String, Integer>> entry : termmap) {
//            w.term = entry.getValue().key;
//            w.documentFrequency = entry.getKey();
//            w.termFrequency = entry.getValue().getValue();
//            w.write(vocabularyFile);
//        }
        vocabularyFile.closeWrite();
    }
}
