package PanDetection;

import PanDetection.AnnChunk.Passage;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static io.github.htools.lib.PrintTools.sprintf;

public class PanReduce extends Reducer<Text, Passage, NullWritable, NullWritable> {

    public static final Log log = new Log(PanReduce.class);
    Conf conf;
    HDFSPath outPath;
    ArchiveFileWriter writer;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);

        // setup a single SimilarityFile that contains the k-most similar source
        // documents for a given suspicious document
        outPath = conf.getHDFSPath("output");
        writer = ArchiveFileWriter.getWriter(outPath.getFile("suspicious.tar.lz4"), 9);
    }

    @Override
    public void reduce(Text key, Iterable<Passage> passages, Context context) throws IOException, InterruptedException {

        // a map that automatically keeps only the items with the top-k highest keys
        // add all similarities for a given suspicious document (key) to a topkmap
        // to select only the top-k most similar source documents
        BufferDelayedWriter buffer = new BufferDelayedWriter();
        buffer.writeRaw("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        buffer.writeRaw("<document reference=\"suspicious-document%05d.txt\">\n", 
                Integer.parseInt(key.toString()));
        for (Passage p : passages) {
            buffer.writeRaw("<feature name=\"detected-plagiarism\" "
                    + "this_offset=\"%d\" this_length=\"%d\" "
                    + "source_reference=\"source-document%05d.txt\" "
                    + "source_offset=\"%d\" source_length=\"%d\" />\n",
                    p.queryoffset, p.querylength, 
                    Integer.parseInt(p.sourceid), p.sourceoffset, p.sourcelength);
        }
        buffer.writeRaw("</document>\n");
        String filename = sprintf("suspicious-document%05d.xml", 
                Integer.parseInt(key.toString()));
        writer.write(filename, buffer.getSize(), buffer.getAsInputStream());
    }
    
    public void cleanup(Context context) throws IOException {
        writer.close();
    }
}
