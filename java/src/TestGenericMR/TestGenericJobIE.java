package TestGenericMR;

import io.github.htools.hadoop.Conf;
import java.io.IOException;

/**
 * @author Jeroen
 */
public class TestGenericJobIE extends TestGenericJob {

    public TestGenericJobIE(Conf conf, String sources, String suspicious, String output) throws IOException {
        super(conf, sources, suspicious, output);
        setMapperClass(TestGenericMapIE.class);
        setReducerClass(TestGenericReduceIE.class);
    }    
}
