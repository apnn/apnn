package SplitAnnotated;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Create vocabulary from collection
 *
 * @author jeroen
 */
public class SplitMap extends Mapper<Object, Text, IntWritable, Text> {

    public static final Log log = new Log(SplitMap.class);
    Conf conf;
    IntWritable key = new IntWritable();
    TreeMap<Integer, Integer> idMap;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        idMap = readIds();
    }

    @Override
    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        String[] part = line.toString().split("\\t");
        if (part.length > 1) {
            int id = Integer.parseInt(part[0]);
            this.key.set(idMap.ceilingEntry(id).getValue());
            context.write(this.key, line);
        }
    }

    public TreeMap<Integer, Integer> readIds() {
        TreeMap<Integer, Integer> map = new TreeMap();
        for (String line : conf.getHDFSFile("ids").readLines()) {
            String[] part = line.split("\\s+");
            map.put(Integer.parseInt(part[2]), map.size());
        }
        return map;
    }
}
