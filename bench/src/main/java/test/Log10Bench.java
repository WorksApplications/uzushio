package test;

import org.apache.commons.math3.util.FastMath;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;

@State(Scope.Benchmark)
public class Log10Bench {
    double[] arg;

    @Setup
    public void setup() {
        double[] arr = new double[10000];
        Random rng = new Random(42L);
        for (int i = 0; i < 10000; ++i) {
            arr[i] = rng.nextInt(10000) + 1;
        }
        arg = arr;
    }


    @Benchmark
    public double bulitin() {
        double result = 0;
        double[] arr = arg;
        for (int i = 0; i < 10000; i++) {
            double v = arr[i];
            result += Math.log10(v);
        }
        return result;
    }

    @Benchmark
    public double fastMath() {
        double result = 0;
        double[] arr = arg;
        for (int i = 0; i < 10000; i++) {
            double v = arr[i];
            result += FastMath.log10(v);
        }
        return result;
    }
}
