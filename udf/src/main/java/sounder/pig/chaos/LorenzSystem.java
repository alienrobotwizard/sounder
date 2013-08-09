package sounder.pig.chaos;

/**
   A simple and well known continuous time dynamical system.
   See: http://en.wikipedia.org/wiki/Lorenz_system
   <p>
   lyapunov exponent: ~0.91
 */
public class LorenzSystem extends DynamicalSystem {

    private double sigma;
    private double beta;
    private double rho;
    
    public LorenzSystem(double sigma, double beta, double rho, double[] x0) {
        super(x0, 0.01);    // Hard code a reasonably small step size. 
        this.sigma = sigma;
        this.beta = beta;
        this.rho = rho;
    }

    @Override
    public void computeDerivatives(double t, double[] x, double[] xDot) {
        xDot[0] = sigma*(x[1] - x[0]);
        xDot[1] = x[0]*(rho - x[2]) - x[1];
        xDot[2] = x[0]*x[1] - beta*x[2];
    }
}
