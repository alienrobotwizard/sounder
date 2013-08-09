package sounder.pig.chaos;

/**
   One of the simplest chaotic systems (it's 2D); used
   primarily for quick verification of analytic measures
   such as the lyapunov exponent.
   <p>
   lyapunov exponent: ~0.42
 */
public class Henon extends DynamicalSystem {

    private double a;
    private double b;

    /**
       Initialize the Henon Map with parameters a and b
       and initial conditions x0. Since this is a discrete
       time dynamical system, set the timestep to 1.0
     */
    public Henon(double a, double b, double[] x0) {
        super(x0, 1.0); 
        this.a = a;
        this.b = b;
    }

    /**
       This is not a derivative in the strict sense since time
       for this system is discrete.
     */
    @Override
    public void computeDerivatives(double t, double[] x, double[] xDot) {
        xDot[0] = x[1] + 1.0 - a*Math.pow(x[0], 2);
        xDot[1] = b*x[0];
    }

    /**
       Here it's necessary to override the evolve methods since,
       again, this is a discrete time dynamical system and
       no integration is necessary.
     */
    @Override
    public double[] evolve() {
        double[] evolved = new double[getDimension()];
        computeDerivatives(0, x, evolved);
        System.arraycopy(evolved, 0, x, 0, getDimension());
        return x;
    }

    @Override
    public double[] evolveOther(double[] xOther) {
        double[] evolved = new double[getDimension()];
        computeDerivatives(0, xOther, evolved);
        return evolved;
    }
}
