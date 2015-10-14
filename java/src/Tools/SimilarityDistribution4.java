package Tools;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Jeroen
 */
public class SimilarityDistribution4 {

    public static Log log = new Log(SimilarityDistribution4.class);

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "input input2");
        HashMap<Integer, double[]> d1 = getRelativeDistance(conf.getHDFSFile("input"));
        HashMap<Integer, double[]> d2 = getRelativeDistance(conf.getHDFSFile("input2"));
        double kld[] = new double[100];
        int count[] = new int[100];
        for (Map.Entry<Integer, double[]> entry : d1.entrySet()) {
            double[] dd = d2.get(entry.getKey());
            if (dd != null) {
                for (int i = 2; i < 100; i++) {
                    if (entry.getValue().length >= i && dd.length >= i) {
                        double a[] = ArrayTools.subArray(entry.getValue(), 0, i);
                        a = DoubleTools.normalize(a);
                        double b[] = ArrayTools.subArray(dd, 0, i);
                        b = DoubleTools.normalize(b);
                        double k = DoubleTools.JensenShannonDivergence(a, b);
                        kld[i] += k;
                        count[i]++;
                    }
                }
            }
        }
        for (int i = 2; i < 100; i++) {
            log.printf("%d %f", i, kld[i] / count[i]);
        }
    }

    public static HashMap<Integer, double[]> getRelativeDistance(Datafile file) {
        HashMapList<Integer, Double> map = new HashMapList();
        file.setBufferSize(1000000);
        SimilarityFile similarityFile = new SimilarityFile(file);
        for (SimilarityWritable s : similarityFile) {
                map.add(s.id, s.score);
        }

        HashMap<Integer, double[]> results = new HashMap();
        for (Map.Entry<Integer, ArrayList<Double>> entry : map.entrySet()) {
            double[] list = ArrayTools.toDoubleArray(entry.getValue());
            double[] z = DoubleTools.normalize(list);
            results.put(entry.getKey(), z);
        }
        return results;
    }
}
