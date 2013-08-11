package sounder.pig.chaos;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
   A simple eval func to compute the lyapunov exponent
   for the Henon map with parameters supplied as arguments
   to the function
 */
public class LyapunovForHenon extends EvalFunc<Double> {

    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;

        Double a = (Double)input.get(0);
        Double b = (Double)input.get(1);

        // Start system at origin
        double[] x0 = new double[2];
        x0[0] = 0.0;
        x0[1] = 0.0;
        
        DynamicalSystem henonMap = new Henon(a, b, x0);
        Double result = new Double(Lyapunov.computeLargestExponent(henonMap));
        return result;        
    }
}
