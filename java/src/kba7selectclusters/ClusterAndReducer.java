package kba7selectclusters;

import kba7clusterand.*;
import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jeroen
 */
public class ClusterAndReducer extends Reducer<IntLongWritable, StreamClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterAndReducer.class);
    HashMap<Integer, ArrayList<ClusterWritable>> map = new HashMap();
    StreamClusterFile cf;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        HDFSPath path = new HDFSPath(conf, conf.get("output"));
        Datafile df = path.getFile(sprintf("topic.%d", this.getTopicID(conf, ContextTools.getTaskID(context))));
        cf = new StreamClusterFile(df);
    }

    @Override
    public void reduce(IntLongWritable key, Iterable<StreamClusterWritable> values, Context context) throws IOException, InterruptedException {
        for (StreamClusterWritable value : values) {
            log.info("%d", value.clusterid);
            value.write(cf);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        cf.closeWrite();
    }

    public int getTopicID(Configuration conf, int topic) {
        TopicFile tf = new TopicFile(new Datafile(conf, conf.get("topicfile")));
        for (TopicWritable t : tf) {
            if (topic-- == 0) {
                tf.closeRead();
                return t.id;
            }
        }
        return -1;
    }
}
