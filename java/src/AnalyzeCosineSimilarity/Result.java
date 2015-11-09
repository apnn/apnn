package AnalyzeCosineSimilarity;

import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.TopKMap;
import io.github.htools.hadoop.io.buffered.Writable;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.util.Map;

/**
 *
 * @author Jeroen
 */
public class Result extends Writable {
    public static Log log = new Log(Result.class);
    double similarity;
    double magnitude;
    int docid;
    int id;
    ArrayMap<Double, String> map;
    
    public void set(int id, int docid, double similarity, double magnitude, TopKMap<Double, String> map) {
        this.id = id;
        this.docid = docid;
        this.similarity = similarity;
        this.magnitude = magnitude;
        this.map = new ArrayMap();
        this.map.addAll(map);
    }
    
    @Override
    public Result clone() {
        Result clone = new Result();
        clone.docid = docid;
        clone.id = id;
        clone.magnitude = magnitude;
        clone.similarity = similarity;
        clone.map = map;
        return clone;
    }

    @Override
    public void write(BufferDelayedWriter writer) {
        writer.write(id);
        writer.write(docid);
        writer.write(similarity);
        writer.write(magnitude);
        writer.write(map.size());
        for (Map.Entry<Double, String> entry : map) {
            writer.write(entry.getKey());
            writer.write(entry.getValue());
        }
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        id = reader.readInt();
        docid = reader.readInt();
        similarity = reader.readDouble();
        magnitude = reader.readDouble();
        int size = reader.readInt();
        map = new ArrayMap(size);
        for (int i = 0; i < size; i++) {
            map.add(reader.readDouble(), reader.readString());
        }
    }
    
    public String toString() {
        return sprintf("%d %d %f %f %s", id, docid, similarity, magnitude, map);
    }

}
