package sounder.pig.chaos;

import org.apache.commons.math3.ode.FirstOrderDifferentialEquations;
import org.apache.commons.math3.ode.ExpandableStatefulODE;
import org.apache.commons.math3.ode.nonstiff.ClassicalRungeKuttaIntegrator;

public class DynamicalSystem implements FirstOrderDifferentialEquations {

    protected double[] x;
    protected double t;
    protected double dt;
    protected int dimension;
    private ClassicalRungeKuttaIntegrator solver;
    private ExpandableStatefulODE internalState;
    
    public DynamicalSystem(double[] x0, double dt) {
        this.dimension = x0.length;
        this.x = new double[x0.length];
        this.dt = dt;
        this.t = 0.0;
        System.arraycopy(x0, 0, this.x, 0, x0.length);

        this.solver = new ClassicalRungeKuttaIntegrator(this.dt);
        this.internalState = new ExpandableStatefulODE(this);
        this.internalState.setTime(this.t);
        this.internalState.setCompleteState(this.x);
    }

    public void computeDerivatives(double t, double[] x, double[] xDot) {
    }

    public int getDimension() {
        return dimension;
    }

    public double[] evolve() {
        solver.integrate(internalState, t+dt);
        this.t = t+dt;
        this.x = internalState.getCompleteState();
        return x;
    }

    public double[] getState() {
        return x;
    }

    public double getTime() {
        return t;
    }
}
