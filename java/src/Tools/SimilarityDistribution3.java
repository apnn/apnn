package Tools;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.IntTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Jeroen
 */
public class SimilarityDistribution3 {

    public static Log log = new Log(SimilarityDistribution3.class);

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "input input2");
        HashMap<Integer, Double> d1 = getRelativeDistance(conf.getHDFSFile("input"));
        HashMap<Integer, Double> d2 = getRelativeDistance(conf.getHDFSFile("input2"));
        for (Map.Entry<Integer, Double> entry : d1.entrySet()) {
            Double dd = d2.get(entry.getKey());
            if (dd != null) {
                log.printf("%f %f", entry.getValue(), dd);
            }
        }
    }
    
    public static HashMap<Integer, Double> getRelativeDistance(Datafile file) {
        HashMapList<Integer, Double> map = new HashMapList();
        file.setBufferSize(1000000);
        SimilarityFile similarityFile = new SimilarityFile(file);
        for (SimilarityWritable s : similarityFile) {
            if (map.getList(s.id).size() < 11)
            map.add(s.id, s.score);
        }
        
        HashMap<Integer, Double> results = new HashMap();
        for (Map.Entry<Integer, ArrayList<Double>> entry : map.entrySet()) {
            double[] list = ArrayTools.toDoubleArray(entry.getValue());
            if (list.length > 10) {
                double mean = DoubleTools.mean(list, 1, 11);
                double sd = DoubleTools.standardDeviation(list, 1, 11, mean);
                double p = (list[0] - mean)/sd;
                results.put(entry.getKey(), p);
            }
        }
        return results;
    }
}
