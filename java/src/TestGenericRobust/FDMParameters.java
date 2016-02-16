package TestGenericRobust;

import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.HashMapInt;
import io.github.htools.collection.HashMapList;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorInt;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * @author Jeroen
 */
public class FDMParameters {

    public static Log log = new Log(FDMParameters.class);
    public final double lambdai;
    public final double lambdao;
    public final double lambdau;

    public FDMParameters(Configuration conf) {
        lambdai = conf.getDouble("lambdai", 0.8);
        lambdao = conf.getDouble("lambdao", 0.1);
        lambdau = 1 - lambdai - lambdao;
    }
}
