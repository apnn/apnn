package SplitAnnotated;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class SplitReduce extends Reducer<IntWritable, Text, NullWritable, NullWritable> {

    public static final Log log = new Log(SplitReduce.class);
    Conf conf;
    Datafile outfile;
    int id;
    ArrayList<String> ids;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        ids = readIds();
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
        if (outfile == null || id != key.get()) {
            cleanup(context);
            id = key.get();
            outfile = conf.getHDFSPath("output").getFile(ids.get(id));
            outfile.openWrite();
        }
        for (Text line : lines) {
            outfile.printf("%s\n", line.toString());
        }
    }
    
    public void cleanup(Context context) throws IOException {
        if (outfile != null)
           outfile.close();
    }

    public ArrayList<String> readIds() {
        ArrayList<String> ids = new ArrayList();
        ByteSearch filename = ByteSearch.create("[0-9\\.]+");
        for (String line : conf.getHDFSFile("ids").readLines()) {
            ids.add(filename.extract(line));
        }
        return ids;
    }
}
