package VocabularyPAN;

import io.github.htools.collection.ArrayMap3;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.lib.Log;
import io.github.htools.type.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class VocabularyReduce extends Reducer<Text, LongWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(VocabularyReduce.class);
    ArrayMap3<Integer, String, Long> termmap = new ArrayMap3();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int documentFrequency = 0;
        long termFrequency = 0;
        if (key.toString().equals(VocabularyMap.COLLECTIONSIZE)) {
            for (LongWritable entry : values) {
                documentFrequency++;
                termFrequency += entry.get();
            }
        } else {
            for (LongWritable entry : values) {
                if (entry.get() > 0) { // suspicious documents have freq=0
                    documentFrequency++;
                    termFrequency += entry.get();
                }
            }
        }
        termmap.add(documentFrequency, key.toString(), termFrequency);
    }

    @Override
    public void cleanup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        VocabularyFile vocabularyFile = new VocabularyFile(conf.getHDFSFile("output"));
        vocabularyFile.setBufferSize(1000000);
        vocabularyFile.openWrite();
        VocabularyWritable w = new VocabularyWritable();
        termmap.descending();
        for (Map.Entry<Integer, Tuple2<String, Long>> entry : termmap) {
            w.term = entry.getValue().key;
            w.documentFrequency = entry.getKey();
            w.termFrequency = entry.getValue().getValue();
            w.write(vocabularyFile);
        }
        vocabularyFile.closeWrite();
    }
}
