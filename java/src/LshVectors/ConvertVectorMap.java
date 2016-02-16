package LshVectors;

import VocabularyPAN.TermID;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.collection.HashMapInt;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * counts the document frequency of terms occurring in Wikipedia pages
 * (MediaWiki format).
 *
 * @author jeroen
 */
public class ConvertVectorMap extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    public static final Log log = new Log(LshVectors.WpVectorMap.class);
    // tokenizes on non-alphanumeric characters, lowercase, stop words removed, no stemming
    ArrayList<HashMapInt<String>> models = new ArrayList();
    TermID termid;
    Conf conf;

    public void setup(Context context) {
        conf = ContextTools.getConfiguration(context);
        log.info("read vocabulary");
        termid = new TermID(conf.getHDFSFile("vocabulary"));
        log.info("vocabulary read");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // parse wikipedia page into .text, .table and .macro, we only use
        // .text and .macro.
        HashMapInt<String> model = new HashMapInt();
        String[] part = value.toString().split("\\s+");
        for (int i = 0; i < part.length; i += 2) {
            String term = part[i];
            int freq = Integer.parseInt(part[i + 1]);
            model.add(term, freq);
        }
        models.add(model);
    }

    public void cleanup(Context context) {
        Datafile df = conf.getHDFSFile("output");
        df.setBufferSize(1000000);
        df.openWrite();
        HashMapInt<String> background = new HashMapInt();
        for (HashMapInt<String> model : models) {
            background.add(model);
        }

        for (HashMapInt<String> model : models) {
            HashMapDouble<String> result = new HashMapDouble();
            for (Map.Entry<String, Integer> entry : model.entrySet()) {
                int bgi = background.get(entry.getKey());
                if (entry.getValue() > 1) {
                    result.put(entry.getKey(), 1.0);
                }
            }
            for (Map.Entry<String, Integer> entry : background.entrySet()) {
                int bgi = entry.getValue();
                if (bgi > 10) {
                    Integer fg = model.get(entry.getKey());
                    if (fg == null || fg == 0) {
                        result.put(entry.getKey(), -1.0);
                    }
                }
            }
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Double> entry : result.entrySet()) {
                Integer id = termid.get(entry.getKey());
                if (id != null) {
                    sb.append(id).append(":").append(entry.getValue()).append(" ");
                }
            }
            df.printf("%s\n", sb.toString());
        }
        df.closeWrite();
    }

}
