/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.sparkts.api.java;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Random;

import org.apache.commons.math3.analysis.MultivariateFunction;
import org.apache.commons.math3.exception.*;
import org.apache.commons.math.optimization.RealPointValuePair;
import org.apache.commons.math3.optim.*;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link BOBYQAOptimizerCustom}.
 */
public class BOBYQAOptimizerTest {

    static final int DIM = 13;

    @Test(expected = NumberIsTooLargeException.class)
    public void testInitOutofbounds() {
        double[] startPoint = point(DIM,3);
        double[][] boundaries = boundaries(DIM,-1,2);
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,1.0),0.0);
        doTest(new Rosen(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 2000, expected);
    }
    
    @Test(expected = DimensionMismatchException.class)
    public void testBoundariesDimensionMismatch() {
        double[] startPoint = point(DIM,0.5);
        double[][] boundaries = boundaries(DIM+1,-1,2);
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,1.0),0.0);
        doTest(new Rosen(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 2000, expected);
    }

    @Test
    public void testRosen() {
        double[] startPoint = point(DIM,0.1);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,1.0),0.0);
        doTest(new Rosen(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 2000, expected);
     }
    
    @Test
    public void testRescue() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0);
        try {
            doTest(new MinusElli(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 1000, expected);
            fail("An TooManyEvaluationsException should have been thrown");
        } catch(TooManyEvaluationsException e) {
        }
    }

    @Test
    public void testMaximize() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),1.0);
        doTest(new MinusElli(), startPoint, boundaries,
                GoalType.MAXIMIZE, 
                2e-10, 5e-6, 1000, expected);
        boundaries = boundaries(DIM,-0.3,0.3); 
        startPoint = point(DIM,0.1);
        doTest(new MinusElli(), startPoint, boundaries,
                GoalType.MAXIMIZE, 
                2e-10, 5e-6, 1000, expected);
    }

    @Test
    public void testEllipse() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new Elli(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 1000, expected);
     }

    @Test
    public void testElliRotated() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new ElliRotated(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-12, 1e-6, 10000, expected);
    }

    @Test
    public void testCigar() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new Cigar(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 100, expected);
    }

    @Test
    public void testTwoAxes() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new TwoAxes(), startPoint, boundaries,
                GoalType.MINIMIZE, 2*
                1e-13, 1e-6, 100, expected);
     }

    @Test
    public void testCigTab() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new CigTab(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 5e-5, 100, expected);
     }

    @Test
    public void testSphere() {
        double[] startPoint = point(DIM,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new Sphere(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 100, expected);
    }

    @Test
    public void testTablet() {
        double[] startPoint = point(DIM,1.0); 
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new Tablet(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 100, expected);
    }

    @Test
    public void testDiffPow() {
        double[] startPoint = point(DIM/2,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM/2,0.0),0.0);
        doTest(new DiffPow(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-8, 1e-1, 120000, expected);
    }

    @Test
    public void testSsDiffPow() {
        double[] startPoint = point(DIM/2,1.0);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM/2,0.0),0.0);
        doTest(new SsDiffPow(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-2, 1.3e-1, 50000, expected);
    }

    @Test
    public void testAckley() {
        double[] startPoint = point(DIM,0.01);
        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new Ackley(), startPoint, boundaries,
                GoalType.MINIMIZE,
                1e-8, 1e-5, 1000, expected);
    }

    @Test
    public void testRastrigin() {
        double[] startPoint = point(DIM,1.0);

        double[][] boundaries = null;
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,0.0),0.0);
        doTest(new Rastrigin(), startPoint, boundaries,
                GoalType.MINIMIZE, 
                1e-13, 1e-6, 1000, expected);
    }

    @Test
    public void testConstrainedRosen() {
        double[] startPoint = point(DIM,0.1);

        double[][] boundaries = boundaries(DIM,-1,2);
        RealPointValuePair expected =
            new RealPointValuePair(point(DIM,1.0),0.0);
//        for (int i=0; i<100; i++)
        doTest(new Rosen(), startPoint, boundaries,
                GoalType.MINIMIZE,
                1e-13, 1e-6, 2000, expected);
    }

    /**
     * @param func Function to optimize.
     * @param startPoint Starting point.
     * @param boundaries Upper / lower point limit.
     * @param goal Minimization or maximization.
     * @param fTol Tolerance relative error on the objective function.
     * @param pointTol Tolerance for checking that the optimum is correct.
     * @param maxEvaluations Maximum number of evaluations.
     * @param expected Expected point / value.
     */
    private void doTest(MultivariateFunction func,
            double[] startPoint,
            double[][] boundaries,
            GoalType goal,
            double fTol,
            double pointTol,
            int maxEvaluations,
            RealPointValuePair expected) {
        int dim = startPoint.length;
//        MultivariateRealOptimizer optim =
//            new PowellOptimizer(1e-13, Math.ulp(1d));
//        RealPointValuePair result = optim.optimize(100000, func, goal, startPoint);
        BOBYQAOptimizerCustom optimizer = new BOBYQAOptimizerCustom(2*startPoint.length + 1);
        InitialGuess initGuess = new InitialGuess(startPoint);
        MaxIter maxIter = new MaxIter(30000);
        MaxEval maxEval = new MaxEval(maxEvaluations);
        SimpleBounds bounds = null;
        if (boundaries == null) {
            bounds = SimpleBounds.unbounded(startPoint.length);
        } else {
            bounds = new SimpleBounds(boundaries[0], boundaries[1]);
        }
        ObjectiveFunction objectiveFunction = new ObjectiveFunction(func);
        PointValuePair optimal = optimizer.optimize(objectiveFunction, goal, bounds,initGuess, maxIter, maxEval);

        System.out.println(func.getClass().getName() + " = " 
        		+ optimizer.getEvaluations() + " f(");
        for (double x: optimal.getPoint())  System.out.print(x + " ");
        System.out.println(") = " +  optimal.getValue());

        Assert.assertEquals(expected.getValue(),
                optimal.getValue(), fTol);
        for (int i = 0; i < dim; i++) {
            Assert.assertEquals(expected.getPoint()[i],
                    optimal.getPoint()[i], pointTol);
        }
    }

    private static double[] point(int n, double value) {
        double[] ds = new double[n];
        Arrays.fill(ds, value);
        return ds;
    }

    private static double[][] boundaries(int dim,
            double lower, double upper) {
        double[][] boundaries = new double[2][dim];
        for (int i = 0; i < dim; i++)
            boundaries[0][i] = lower;
        for (int i = 0; i < dim; i++)
            boundaries[1][i] = upper;
        return boundaries;
    }

    private static class Sphere implements MultivariateFunction {

        public double value(double[] x) {
            double f = 0;
            for (int i = 0; i < x.length; ++i)
                f += x[i] * x[i];
            return f;
        }
    }

    private static class Cigar implements MultivariateFunction {
        private double factor;

        Cigar() {
            this(1e3);
        }

        Cigar(double axisratio) {
            factor = axisratio * axisratio;
        }

        public double value(double[] x) {
            double f = x[0] * x[0];
            for (int i = 1; i < x.length; ++i)
                f += factor * x[i] * x[i];
            return f;
        }
    }

    private static class Tablet implements MultivariateFunction {
        private double factor;

        Tablet() {
            this(1e3);
        }

        Tablet(double axisratio) {
            factor = axisratio * axisratio;
        }

        public double value(double[] x) {
            double f = factor * x[0] * x[0];
            for (int i = 1; i < x.length; ++i)
                f += x[i] * x[i];
            return f;
        }
    }

    private static class CigTab implements MultivariateFunction {
        private double factor;

        CigTab() {
            this(1e4);
        }

        CigTab(double axisratio) {
            factor = axisratio;
        }

        public double value(double[] x) {
            int end = x.length - 1;
            double f = x[0] * x[0] / factor + factor * x[end] * x[end];
            for (int i = 1; i < end; ++i)
                f += x[i] * x[i];
            return f;
        }
    }

    private static class TwoAxes implements MultivariateFunction {

        private double factor;

        TwoAxes() {
            this(1e6);
        }

        TwoAxes(double axisratio) {
            factor = axisratio * axisratio;
        }

        public double value(double[] x) {
            double f = 0;
            for (int i = 0; i < x.length; ++i)
                f += (i < x.length / 2 ? factor : 1) * x[i] * x[i];
            return f;
        }
    }

    private static class ElliRotated implements MultivariateFunction {
        private Basis B = new Basis();
        private double factor;

        ElliRotated() {
            this(1e3);
        }

        ElliRotated(double axisratio) {
            factor = axisratio * axisratio;
        }

        public double value(double[] x) {
            double f = 0;
            x = B.Rotate(x);
            for (int i = 0; i < x.length; ++i)
                f += Math.pow(factor, i / (x.length - 1.)) * x[i] * x[i];
            return f;
        }
    }

    private static class Elli implements MultivariateFunction {

        private double factor;

        Elli() {
            this(1e3);
        }

        Elli(double axisratio) {
            factor = axisratio * axisratio;
        }

        public double value(double[] x) {
            double f = 0;
            for (int i = 0; i < x.length; ++i)
                f += Math.pow(factor, i / (x.length - 1.)) * x[i] * x[i];
            return f;
        }
    }

    private static class MinusElli implements MultivariateFunction {
        private int fcount = 0;
        public double value(double[] x) {
          double f = 1.0-(new Elli().value(x));
//          System.out.print("" + (fcount++) + ") ");
//          for (int i = 0; i < x.length; i++)
//              System.out.print(x[i] +  " ");
//          System.out.println(" = " + f);
          return f;
       }
    }

    private static class DiffPow implements MultivariateFunction {
        private int fcount = 0;
        public double value(double[] x) {
            double f = 0;
            for (int i = 0; i < x.length; ++i)
                f += Math.pow(Math.abs(x[i]), 2. + 10 * (double) i
                        / (x.length - 1.));
//            System.out.print("" + (fcount++) + ") ");
//            for (int i = 0; i < x.length; i++)
//                System.out.print(x[i] +  " ");
//            System.out.println(" = " + f);
            return f;
        }
    }

    private static class SsDiffPow implements MultivariateFunction {

        public double value(double[] x) {
            double f = Math.pow(new DiffPow().value(x), 0.25);
            return f;
        }
    }

    private static class Rosen implements MultivariateFunction {
        private int fcount = 0;
        public double value(double[] x) {
            double f = 0;
            for (int i = 0; i < x.length - 1; ++i)
                f += 1e2 * (x[i] * x[i] - x[i + 1]) * (x[i] * x[i] - x[i + 1])
                + (x[i] - 1.) * (x[i] - 1.);
//          System.out.print("" + (fcount++) + ") ");
//          for (int i = 0; i < x.length; i++)
//              System.out.print(x[i] +  " ");
//          System.out.println(" = " + f);
            return f;
        }
    }

    private static class Ackley implements MultivariateFunction {
        private double axisratio;

        Ackley(double axra) {
            axisratio = axra;
        }

        public Ackley() {
            this(1);
        }

        public double value(double[] x) {
            double f = 0;
            double res2 = 0;
            double fac = 0;
            for (int i = 0; i < x.length; ++i) {
                fac = Math.pow(axisratio, (i - 1.) / (x.length - 1.));
                f += fac * fac * x[i] * x[i];
                res2 += Math.cos(2. * Math.PI * fac * x[i]);
            }
            f = (20. - 20. * Math.exp(-0.2 * Math.sqrt(f / x.length))
                    + Math.exp(1.) - Math.exp(res2 / x.length));
            return f;
        }
    }

    private static class Rastrigin implements MultivariateFunction {

        private double axisratio;
        private double amplitude;

        Rastrigin() {
            this(1, 10);
        }

        Rastrigin(double axisratio, double amplitude) {
            this.axisratio = axisratio;
            this.amplitude = amplitude;
        }

        public double value(double[] x) {
            double f = 0;
            double fac;
            for (int i = 0; i < x.length; ++i) {
                fac = Math.pow(axisratio, (i - 1.) / (x.length - 1.));
                if (i == 0 && x[i] < 0)
                    fac *= 1.;
                f += fac * fac * x[i] * x[i] + amplitude
                * (1. - Math.cos(2. * Math.PI * fac * x[i]));
            }
            return f;
        }
    }

    private static class Basis {
        double[][] basis;
        Random rand = new Random(2); // use not always the same basis

        double[] Rotate(double[] x) {
            GenBasis(x.length);
            double[] y = new double[x.length];
            for (int i = 0; i < x.length; ++i) {
                y[i] = 0;
                for (int j = 0; j < x.length; ++j)
                    y[i] += basis[i][j] * x[j];
            }
            return y;
        }

        void GenBasis(int DIM) {
            if (basis != null ? basis.length == DIM : false)
                return;

            double sp;
            int i, j, k;

            /* generate orthogonal basis */
            basis = new double[DIM][DIM];
            for (i = 0; i < DIM; ++i) {
                /* sample components gaussian */
                for (j = 0; j < DIM; ++j)
                    basis[i][j] = rand.nextGaussian();
                /* substract projection of previous vectors */
                for (j = i - 1; j >= 0; --j) {
                    for (sp = 0., k = 0; k < DIM; ++k)
                        sp += basis[i][k] * basis[j][k]; /* scalar product */
                    for (k = 0; k < DIM; ++k)
                        basis[i][k] -= sp * basis[j][k]; /* substract */
                }
                /* normalize */
                for (sp = 0., k = 0; k < DIM; ++k)
                    sp += basis[i][k] * basis[i][k]; /* squared norm */
                for (k = 0; k < DIM; ++k)
                    basis[i][k] /= Math.sqrt(sp);
            }
        }
    }
}
