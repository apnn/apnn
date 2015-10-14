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
public class SimilarityDistribution5 {

    public static Log log = new Log(SimilarityDistribution5.class);

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "input input2");
        HashMap<Integer, double[]> d1 = getRelativeDistance(conf.getHDFSFile("input"));
        HashMap<Integer, double[]> d2 = getRelativeDistance(conf.getHDFSFile("input2"));
        ArrayList<Double> kld = new ArrayList();
        
        for (Map.Entry<Integer, double[]> entry : d1.entrySet()) {
            double[] d = entry.getValue();
            double[] dd = d2.get(entry.getKey());
            if (dd != null) {
                for (int i = 0; i < 100 && i < d.length && i < dd.length; i++) {
                    log.printf("%f\t%f \\\\", d[i], dd[i]);
                }
            }
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
            //log.info("%s\n\n%s\n\n", ArrayTools.toString(list), ArrayTools.toString(z));
            results.put(entry.getKey(), z);
        }
        return results;
    }
}
