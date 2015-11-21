package TestGenericMR;

import TestGenericMR.SourceQueryPairInputFormat.Pair;
import io.github.htools.hadoop.io.ConstInputFormat;
import io.github.htools.hadoop.io.MRInputSplit;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.buffered.Writable;
import io.github.htools.io.Datafile;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.Log;

/**
 * Custom InputFormat that feeds each Map with a <Integer, Pair> pair, in which
 * the Integer is a unique sequence number and the Pair contains a (partial) source
 * and query file: the pathname to a file with offset and length.
 * @author Jeroen
 */
public class SourceQueryPairInputFormat extends ConstInputFormat<Integer, Pair> {

    public static Log log = new Log(SourceQueryPairInputFormat.class);

    @Override
    protected InputSplit createSplit(Integer key) {
        return new InputSplit(key);
    }
    
    public static void add(Job job, String sourcefile, String suspiciousfile) {
        Pair value = new Pair();
        value.sourcefile = sourcefile;
        value.queryfile = suspiciousfile;
        SourceQueryPairInputFormat.add(job, SourceQueryPairInputFormat.size(), value);
    }
    
    public static void add(Job job, Datafile sourcefile, Datafile suspiciousfile) {
        Pair value = new Pair();
        value.sourcefile = sourcefile.getCanonicalPath();
        value.queryfile = suspiciousfile.getCanonicalPath();
        value.sourceoffset = sourcefile.getOffset();
        value.sourceend = sourcefile.getCeiling();
        value.suspiciousoffset = suspiciousfile.getOffset();
        value.suspiciousend = suspiciousfile.getCeiling();
        SourceQueryPairInputFormat.add(job, SourceQueryPairInputFormat.size(), value);
    }
    
    public static class Pair extends Writable {

        public String sourcefile;
        public String queryfile;
        public long sourceoffset;
        public long sourceend;
        public long suspiciousoffset;
        public long suspiciousend;
        
        @Override
        public void write(BufferDelayedWriter writer) {
           writer.write(sourcefile);
           writer.write(queryfile);
           writer.write(sourceoffset);
           writer.write(sourceend);
           writer.write(suspiciousoffset);
           writer.write(suspiciousend);
        }

        @Override
        public void readFields(BufferReaderWriter reader) {
            sourcefile = reader.readString();
            queryfile = reader.readString();
            sourceoffset = reader.readLong();
            sourceend = reader.readLong();
            suspiciousoffset = reader.readLong();
            suspiciousend = reader.readLong();
        }
    }

    public static class InputSplit extends MRInputSplit<Integer, Pair> {
        Pair value = new Pair();

        public InputSplit() {
            super();
        }

        public InputSplit(Integer key) {
            super(key);
        }

        @Override
        public void writeValue(BufferDelayedWriter out, Pair value) {
            value.write(out);
        }

        @Override
        public Pair readValue(BufferReaderWriter reader) {
            value.readFields(reader);
            return value;
        }

        @Override
        public void writeKey(BufferDelayedWriter out, Integer key) {
            out.write(key);
        }

        @Override
        public Integer readKey(BufferReaderWriter reader) {
            return reader.readInt();
        }
    }
}
