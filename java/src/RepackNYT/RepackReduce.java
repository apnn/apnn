package RepackNYT;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * writes the ground truth file, that contains all labels for annotated documents.
 */
public class RepackReduce extends Reducer<Text, Text, NullWritable, NullWritable> {

    public static final Log log = new Log(RepackReduce.class);
    Conf conf;
    Datafile outfile;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        outfile = conf.getHDFSFile("gt");
        outfile.openWrite();
    }

    @Override
    public void reduce(Text doc, Iterable<Text> labels, Context context) throws IOException, InterruptedException {
        for (Text label : labels) {
            outfile.printf("%s\t%s\n", doc.toString(), label.toString());
        }
    }
    
    public void cleanup(Context context) throws IOException {
        outfile.close();
    }
}
