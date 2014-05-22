/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.LatencyUtils.SimplePauseDetector;

import java.util.Date;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A simple demonstration of using a LatencyStats object to track and report on
 * latencies. In normal, un-stalled execution, this demo should result in simple
 * expected behavior. When stalled manually (e.g. by hitting ^Z, waiting a few
 * seconds, and then continuing execution with fg), latencyStats will detect the
 * pause and compensate for it, which will be evident in the reporting output.
 */
public class LatencyStatsDemo {
    static final long REPORTING_INTERVAL = 2 * 1000 * 1000 * 1000L; // report every 2 sec
    static final long RECORDING_INTERVAL = 5 * 1000 * 1000L; // record every 5 msec, @~200/sec
    static final long OPERATION_LATENCY = 1000 * 1000L; // 1 msec

    // Note that this will create and launch a default pause detector since one has not been set elsewhere:
    static final LatencyStats latencyStats = new LatencyStats();

    /**
     * Records an operation latency (with a fixed OPERATION_LATENCY value) for each call.
     */
    static class Recorder implements Runnable {
        @Override
        public void run() {
            // Record an "observed" latency for a fictitious operation:
            latencyStats.recordLatency(OPERATION_LATENCY);
        }
    }

    /**
     * Samples new interval data and reports on the observed interval histogram
     * and accumulated histogram.
     */
    static class Reporter implements Runnable {
        // We get initial histograms here and get *into* them later, at each sample point.
        //
        // This is an efficiency trick. While we could cleanly get a new histogram set at
        // each  interval sample, and do so with no instance variables to initialie here,
        // doing so would mean that a new set of histograms get constructed per sample.
        // Using the get...Into() variants at each sample, into previously allocated histograms
        // allows us to sample with no allocation.
        //
        // Getting the initial histograms with the regular get...() variant is a simple way of
        // initializing the sampling histograms without having to supply Histogram construction
        // parameters to match latencyStats settings..

        Histogram intervalHistogram = latencyStats.getIntervalHistogram();
        Histogram accumulatedHistogram = latencyStats.getAccumulatedHistogram();

        @Override
        public void run() {
            // Force an interval sample. Without this, the histograms we get would be the same
            // as before...
            latencyStats.forceIntervalSample();

            // Get the histograms (without allocating new ones):
            latencyStats.getIntervalHistogramInto(intervalHistogram);
            latencyStats.getAccumulatedHistogramInto(accumulatedHistogram);

            // Report:
            System.out.println("\n\n--------------\n# Current Time: " + new Date());
            System.out.println("\nInterval Histogram : \n");
            intervalHistogram.getHistogramData().outputPercentileDistribution(System.out, 1000000.0);
            System.out.println("\nAccumulated Histogram : \n");
            accumulatedHistogram.getHistogramData().outputPercentileDistribution(System.out, 1000000.0);
        }
    }

    public static void main(final String[] args)  {
        // Knowing that we're using a SimplePauseDetector, set it to verbose so that the user can see
        // the pause detection messages:
        ((SimplePauseDetector) latencyStats.getPauseDetector()).setVerbose(true);

        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);

        // Record latencies on a "regular" basis. This will tend to record semi-reliably at
        // each interval as long as the JVM doesn't stall.
        executor.scheduleWithFixedDelay(new Recorder(), RECORDING_INTERVAL, RECORDING_INTERVAL, TimeUnit.NANOSECONDS);

        // Regularly report o observations:
        executor.scheduleWithFixedDelay(new Reporter(), REPORTING_INTERVAL, REPORTING_INTERVAL, TimeUnit.NANOSECONDS);

        while (true);
    }
}
