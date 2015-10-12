package TestAnnMR;

import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGenericMR.TestGenericJob;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.ClassTools;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.conf.Configuration;

/**
 * Computes the cosine similarity between all suspicious and source documents of
 * the PAN11 collection. Using this class requires to at least set the AnnIndex
 * to be used, and also {@link TestGenericJob} to optionally set k (default=100)
 * as the maximum number of most similar source documents to retrieve and set
 * the similarity function (default=CosineSimilarity) that is used to score the
 * similarity between two documents.
 *
 * The default mapper constructs an index of source documents, and then inspects
 * every suspicious document for matching source documents.
 *
 * The default reducer keeps only the k-most similar source document per
 * suspicious document and stores the result in a SimilarityFile. Override the
 * configured Mapper and Reducer to change the default operation.
 *
 * parameters:
 *
 * sources: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) suspicious: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4) output: the
 * resulting k-most similar source documents per suspicious document are written
 * to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class TestAnnJob extends TestGenericJob {

    private static final Log log = new Log(TestAnnJob.class);
    public static final String ANNINDEXCLASS = TestAnnJob.class.getCanonicalName() + ".AnnIndexClass";

    public TestAnnJob(Conf conf, String sources, String suspicious, String outFile) throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        super(conf, sources, suspicious, outFile);

        // by default use TestAnnMap as mapper, which can be configured by setting
        // setAnnIndex(), setSimilarityFunction() and setTopK().
        this.setMapperClass(TestAnnMap.class);
    }

    /**
     * Configure the implementation of AnnIndex to use
     * @param job
     * @param clazz
     */
    public void setAnnIndex(Class<? extends AnnIndex> clazz) {
        getConfiguration().set(ANNINDEXCLASS, clazz.getCanonicalName());
    }

    /**
     * @param conf
     * @return an instance of the configured implementation to use as AnnIndex,
     * using the configured similarityFunction
     * @throws ClassNotFoundException
     */
    public static AnnIndex getAnnIndex(SimilarityFunction function, Configuration conf) {
        String clazzname = conf.get(ANNINDEXCLASS);
        try {
            Class clazz = ClassTools.toClass(clazzname);

            Constructor<AnnIndex> constructor = ClassTools.getAssignableConstructor(clazz, AnnIndex.class, SimilarityFunction.class, Configuration.class);

            AnnIndex index = ClassTools.construct(constructor, function, conf);
            return index;
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "getAnnIndex %s", clazzname);
            return null;
        }
    }
}
