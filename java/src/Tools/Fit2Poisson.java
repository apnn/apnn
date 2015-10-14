package Tools;

import io.github.htools.lib.Log;
import org.apache.commons.math3.distribution.PoissonDistribution;

/**
 * MLE to Fit a 2-poisson process to an ordered list of observations
 * @author Jeroen
 */
public class Fit2Poisson {
    public static Log log = new Log(Fit2Poisson.class);
    double[] observations;
    double p;
    PoissonDistribution poisson1;
    PoissonDistribution poisson2;

    public Fit2Poisson(double[] observations) {
        this.observations = observations;
    }
    
    public double doublePoisson(int x) {
        return p * poisson1.probability(x) + (1-p) * poisson2.probability(x);
    }
    
//    public void expectation() {
//        double newp = p * poisson1.cumulativeProbability(observations.length-1) /
//              observations.length *   
//    }
    
    
}
