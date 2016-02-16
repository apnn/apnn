package RepackPAN11;

import TestGeneric.ContentExtractor;
import TestGeneric.ContentExtractorPAN;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.io.compressed.ArchiveFileWriter;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class RepackMap extends Mapper<Object, String, NullWritable, NullWritable> {

    public static final Log log = new Log(RepackMap.class);
    Conf conf;
    ContentExtractor tokenizer = new ContentExtractorPAN();
    HDFSPath outPath;

    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        outPath = conf.getHDFSPath("output");
    }

    @Override
    public void map(Object key, String value, Context context) throws IOException, InterruptedException {
        ArchiveFile inputArchive = ArchiveFile.getReader(conf, value);

        String filename = new HDFSPath(conf, value).getName();
        ArchiveFileWriter outputArchive = ArchiveFileWriter.getWriter(outPath.getFile(filename), 9);
        BufferDelayedWriter buffer = new BufferDelayedWriter();

        for (ArchiveEntry archiveEntry : (Iterable<ArchiveEntry>) inputArchive) {
            byte[] readAll = archiveEntry.readAll();
            ArrayList<String> terms = tokenizer.getTokens(tokenizer.extractContent(readAll));

            for (String term : terms) {
                buffer.writeRaw("%s ", term);
            }
            outputArchive.write(getDocNumber(archiveEntry.getName()), buffer.getSize(), buffer.getAsInputStream());
        }
        outputArchive.close();
    }

    static ByteSearch docNumber = ByteSearch.create("\\d+");

    public static String getDocNumber(String docname) {
        return Integer.toString(Integer.parseInt(docNumber.extract(docname)));
    }
}
