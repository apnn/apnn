package Tools;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;

import java.util.ArrayList;

/**
 *
 * @author Jeroen
 */
public class TransformPANResults {
    public static Log log = new Log(TransformPANResults.class);

    public static void main(String[] args) {
        Conf conf = new Conf(args, "in");
        HDFSPath path = conf.getHDFSPath("in");
        for (Datafile df : path.getFiles()) {
            SimilarityFile f = new SimilarityFile(df);
            f.setBufferSize(1000000);
            ArrayList<SimilarityWritable> list = new ArrayList();
            for (SimilarityWritable w : f) {
                w.query = Integer.toString(Integer.parseInt(w.query));
                w.source = Integer.toString(Integer.parseInt(w.source));
                list.add(w);
            }
            f = new SimilarityFile(new Datafile(conf, df.getCanonicalPath()));
            f.setBufferSize(1000000);
            f.openWrite();
            for (SimilarityWritable w : list) {
                w.write(f);
            }
            f.closeWrite();
        }
    }
    
}
