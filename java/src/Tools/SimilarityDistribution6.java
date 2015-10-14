package Tools;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMap3;
import io.github.htools.collection.HashMapList;
import io.github.htools.collection.HashMapMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.CollectionTools;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.IntTools;
import io.github.htools.lib.Log;
import io.github.htools.type.KV;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.math3.stat.correlation.KendallsCorrelation;

/**
 *
 * @author Jeroen
 */
public class SimilarityDistribution6 {

    public static Log log = new Log(SimilarityDistribution6.class);

    public static void main(String[] args) throws IOException {
        KendallsCorrelation KC = new KendallsCorrelation();
        Conf conf = new Conf(args, "input input2");
        HashMap<Integer, ArrayMap<Double, Integer>> d1 = getRelativeDistance(conf.getHDFSFile("input"));
        HashMap<Integer, ArrayMap<Double, Integer>> d2 = getRelativeDistance(conf.getHDFSFile("input2"));
        ArrayList<Double> taus = new ArrayList();
        ArrayList<Integer> count = new ArrayList();

        for (Map.Entry<Integer, ArrayMap<Double, Integer>> entry : d1.entrySet()) {
            ArrayMap<Double, Integer> d = entry.getValue();
            ArrayMap<Double, Integer> dd = d2.get(entry.getKey());

            if (dd != null) {
                KV<double[], double[]> paired = align(d, dd);
                double tau = KC.correlation(paired.key, paired.value);
                log.info("%d %f %s %s", paired.key.length, tau, 
                        ArrayTools.toString(paired.key), 
                        ArrayTools.toString(paired.value));
                taus.add(tau);
                count.add(paired.key.length);

            }
        }
        log.info("%f %f", IntTools.mean(count), DoubleTools.mean(taus));
    }

    public static HashMap<Integer, ArrayMap<Double, Integer>> getRelativeDistance(Datafile file) {
        HashMap<Integer, ArrayMap<Double, Integer>> map = new HashMap();
        file.setBufferSize(1000000);
        SimilarityFile similarityFile = new SimilarityFile(file);
        for (SimilarityWritable s : similarityFile) {
            ArrayMap<Double, Integer> submap = map.get(s.id);
            if (submap == null) {
                submap = new ArrayMap();
                map.put(s.id, submap);
            }
            submap.add(s.score, s.source);
        }
        for (ArrayMap<Double, Integer> submap : map.values()) {
            submap.descending();
        }
        return map;
    }

    public static KV<double[], double[]> align(ArrayMap<Double, Integer> map1,
            ArrayMap<Double, Integer> map2) {
        HashMap3<Integer, Double, Double> m = new HashMap3();
        for (int i = 0; i < 3 && i < map1.size(); i++) {
            int id = map1.getValue(i);
            m.put(id, map1.getKey(i), 0.0);
            for (Map.Entry<Double, Integer> entry3 : map2) {
                if (entry3.getValue() == id) {
                    m.get(id).value = entry3.getKey();
                    break;
                }
            }
        }
        for (int i = 0; i < 3 && i < map2.size(); i++) {
            int id = map2.getValue(i);
            KV<Double, Double> get = m.get(id);
            if (get == null) {
                m.put(map2.getValue(i), 0.0, map2.getKey(i));
                for (Map.Entry<Double, Integer> entry3 : map1) {
                    if (entry3.getValue() == id) {
                        m.get(entry3.getValue()).key = entry3.getKey();
                        break;
                    }
                }
            } else {
                get.value = map2.getKey(i);
            }
        }
        double d1[] = new double[m.size()];
        double d2[] = new double[m.size()];
        int pos = 0;
        for (KV<Double, Double> entry : m.values()) {
            d1[pos] = entry.key;
            d2[pos] = entry.value;
            pos++;
        }

        return new KV<double[], double[]>(d1, d2);
    }
}
