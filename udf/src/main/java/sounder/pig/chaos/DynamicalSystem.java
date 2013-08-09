package sounder.pig.chaos;

import org.apache.commons.math3.util.MathArrays;

/**
   Simple base class for representing dynamical systems. Uses the
   standard fourth order Runge Kutta method for integration.
 */
public class DynamicalSystem {

    protected double[] x;
    protected double t;
    protected double dt;
    protected int dimension;

    // Integration variables
    private double[] k1;
    private double[] k2;
    private double[] k3;
    private double[] k4;
    
    public DynamicalSystem(double[] x0, double dt) {
        this.dimension = x0.length;
        this.x = new double[x0.length];
        this.dt = dt;
        this.t = 0.0;
        this.k1 = new double[x0.length];
        this.k2 = new double[x0.length];
        this.k3 = new double[x0.length];
        this.k4 = new double[x0.length];
        
        System.arraycopy(x0, 0, this.x, 0, x0.length);
    }

    // To be overriden by subclasses
    public void computeDerivatives(double t, double[] x, double[] xDot) {
    }

    public int getDimension() {
        return dimension;
    }

    /**
       Evolve the system by exactly one timestep
     */
    public double[] evolve() {
        this.x = integrate(x, t, dt, 1);
        this.t += dt;                           
        return x;
    }

    /**
       Use this system to evolve another point using the
       same time and step size
     */
    public double[] evolveOther(double[] xOther) {
        double[] result = integrate(xOther, t, dt, 1);  
        return result;
    }

    /**
       Standard 4th order Runge-Kutta method as outlined here:
       http://en.wikipedia.org/wiki/Runge%E2%80%93Kutta_methods
       <p>
       While it is possible to use the commons-math ode package
       instead, this keeps the integration visible and doesn't
       require instantiation of complex objects
     */
    private double[] integrate(double[] x, double tStart, double dt, int num) {
        double[] evolved = new double[x.length];
        System.arraycopy(x, 0, evolved, 0, x.length);
        
        double t = tStart;
        for (int i = 0; i < num; i++) {
            computeDerivatives(t, evolved, k1); // k1
            
            double[] xPlusK1 = MathArrays.ebeAdd( MathArrays.scale(dt/2.0, k1), evolved);
        
            computeDerivatives(t+0.5*dt, xPlusK1, k2);

            double[] xPlusK2 = MathArrays.ebeAdd( MathArrays.scale(dt/2.0, k2), evolved);

            computeDerivatives(t+0.5*dt, xPlusK2, k3);

            double[] xPlusK3 = MathArrays.ebeAdd( MathArrays.scale(dt, k3), evolved);

            computeDerivatives(t+dt, xPlusK3, k4);

            double[] kSum = MathArrays.ebeAdd(MathArrays.ebeAdd(k1, k4), MathArrays.scale(2.0, MathArrays.ebeAdd(k2, k3)));
            evolved = MathArrays.ebeAdd(evolved, MathArrays.scale(dt/6.0,kSum));
            t += dt;
        }
        return evolved;
    }

    /**
       Returns the current state vector of this system
     */
    public double[] getState() {
        return x;
    }

    /**
       Returns the current timestep for this system
     */
    public double getTime() {
        return t;
    }

    /**
       Returns the timestep size for this system
     */
    public double getStepSize() {
        return dt;
    }

    /**
       A convenience method for returning a state vector
       as a printable string
     */
    public static String stateToString(double[] state) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < state.length; i++) {
            b.append(String.valueOf(state[i]));
            if (i != state.length-1) {
                b.append(",");
            }
        }
        return b.toString();
    }
}
