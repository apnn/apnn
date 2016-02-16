package TestIG;

import SimilarityFunction.InformationGain;
import TestGenericMR.TestGenericJob;
import TestGenericNYT.TestGenericNYTJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.Log;

import java.io.IOException;

public class TestIGJob extends TestGenericNYTJob {

    private static final Log log = new Log(TestGenericNYT.TestGenericNYTJob.class);

    public TestIGJob(Conf conf, String source, String output, String vocabulary) throws IOException {
        super(conf, source, output, vocabulary);
        TestGenericJob.setSimilarityFunction(conf, InformationGain.class);
    }
}
