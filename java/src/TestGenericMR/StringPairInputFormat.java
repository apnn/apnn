package TestGenericMR;

import TestGenericMR.StringPairInputFormat.Value;
import io.github.htools.hadoop.io.ConstInputFormat;
import io.github.htools.hadoop.io.MRInputSplit;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.buffered.Writable;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.Log;

/**
 * Custom InputFormat that feeds each Map with a <Integer, Value> pair, in which
 * the Integer is a unique sequence number and the Value contains two Strings:
 * the pathname to a source file and the pathname to a suspicious file. Both files
 * are supposed to be in ArchiveFile format (e.g. .tar.lz4). Each file in the archive
 * is a text document, and the filename contains the document ID.
 * @author Jeroen
 */
public class StringPairInputFormat extends ConstInputFormat<Integer, Value> {

    public static Log log = new Log(StringPairInputFormat.class);

    @Override
    protected InputSplit createSplit(Integer key) {
        return new InputSplit(key);
    }
    
    public static void add(Job job, String sourcefile, String suspiciousfile) {
        Value value = new Value();
        value.sourcefile = sourcefile;
        value.suspiciousfile = suspiciousfile;
        StringPairInputFormat.add(job, StringPairInputFormat.size(), value);
    }
    public static class Value extends Writable {

        public String sourcefile;
        public String suspiciousfile;
        
        @Override
        public void write(BufferDelayedWriter writer) {
           writer.write(sourcefile);
           writer.write(suspiciousfile);
        }

        @Override
        public void readFields(BufferReaderWriter reader) {
            sourcefile = reader.readString();
            suspiciousfile = reader.readString();
        }
    }

    public static class InputSplit extends MRInputSplit<Integer, Value> {
        Value value = new Value();

        public InputSplit() {
            super();
        }

        public InputSplit(Integer key) {
            super(key);
        }

        @Override
        public void writeValue(BufferDelayedWriter out, Value value) {
            value.write(out);
        }

        @Override
        public Value readValue(BufferReaderWriter reader) {
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
