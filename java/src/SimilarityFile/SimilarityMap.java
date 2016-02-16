package SimilarityFile;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Every map receives one Value as input, that contains a unique combination of
 * a filename with a block of suspicious documents and a filename with a block
 * of source documents. The default operation is to do a brute force comparison
 * by reading all source documents into memory, and
 * then the suspicious documents are inspected one-at-a-time for matches in the
 * index. The k-most similar source documents are send to the reducer. This
 * generic mapper can be configured by setting the index, similarity function
 * and k in the job, or overridden to add functionality.
 *
 * @author jeroen
 */
public class SimilarityMap extends Mapper<Object, SimilarityWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(SimilarityMap.class);
    protected Conf conf;
    SimilarityFile out;

    public void setup(Context context) {
        Conf conf = ContextTools.getConfiguration(context);
        String name = ContextTools.getInputPath(context).getName();
        Datafile df = conf.getHDFSPath("output").getFile(name);
        out = new SimilarityFile(df);
        out.setBufferSize(100000000);
        out.openWrite();
    }

    @Override
    public void map(Object key, SimilarityWritable value, Context context) throws IOException, InterruptedException {
        if (!value.source.equals("274144")) {
            value.write(out);
        }
    }

    public void cleanup(Context context) {
        out.closeWrite();
    }
}
