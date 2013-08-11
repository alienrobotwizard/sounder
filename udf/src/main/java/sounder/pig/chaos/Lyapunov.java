package sounder.pig.chaos;

import org.apache.commons.math3.util.MathArrays;

/**
   A class for computing the maximal lyapunov exponent of
   a dynamical system
 */
public class Lyapunov {

    /**
       Given a DynamicalSystem, returns the maximul lyapunov exponent
       using the method outline by J.C. Sprott see:
       http://sprott.physics.wisc.edu/chaos/lyapexp.htm
     */
    public static double computeLargestExponent(DynamicalSystem system) {
        
        double d0 = 10e-8;
                
        // First, evolve the system such that it's very likely to be on
        // the attractor (1000 iterations is a decent guess)
        evolveSystem(system, 10000);

        // Next, pick a test point a distance d0 away
        double[] testPoint = getTestPoint(system, d0);
        double[] x = system.getState();
                           
        double d1 = 0.0;
        double l = 0.0;

        // Evolve both the test point and the original
        for (int i = 0; i < 64100; i++) {

            testPoint = system.evolveOther(testPoint);
            x = system.evolve();

            // Test for unbounded orbits
            if (isUnbounded(x)) {
                return Double.NaN;
            }
            
            // Compute distance
            d1 = MathArrays.distance(testPoint, x);
            
            // Compute lyapunov exponent this round
            if (i > 100) {
                l = l + Math.log(d1/d0);
            }

            // Renormalize test point
            for (int j = 0; j < x.length; j++) {
                testPoint[j] = x[j] + (d0/d1)*(testPoint[j] - x[j]);
            }
        }
        return l/(63999*system.getStepSize());
    }

    private static void evolveSystem(DynamicalSystem system, int times) {
        for (int i = 0; i < times; i++) {
            system.evolve();
        }
    }

    private static double[] getTestPoint(DynamicalSystem system, double d0) {
        double[] testPoint = new double[system.getDimension()];
        double denom = Math.sqrt((new Integer(system.getDimension())).doubleValue());
        double[] x = system.getState();
        
        for (int i = 0; i < x.length; i++) {
            testPoint[i] = x[i] + d0/denom;
        }

        return testPoint;
    }

    private static boolean isUnbounded(double[] x) {
        double absSum = 0.0;
        for (int i = 0; i < x.length; i++) {
            absSum += Math.abs(x[i]);
        }
        if (absSum >= 10e6) {
            return true;
        } else {
            return false;
        }
    }
}
