/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/**
 * JUnit test for {@link org.LatencyUtils.SimplePauseDetector}
 */
public class LatencyStatsTest {

    static {
        System.setProperty("LatencyUtils.useActualTime", "false");
    }

    static final long highestTrackableValue = 3600L * 1000 * 1000 * 1000; // e.g. for 1 hr in nsec units
    static final int numberOfSignificantValueDigits = 2;

    static final long MSEC = 1000000L; // MSEC in nsec units

    @Test
    public void testLatencyStats() throws Exception {

        SimplePauseDetector pauseDetector = new SimplePauseDetector(1000000L /* 1 msec sleep */,
                10000000L /* 10 msec reporting threshold */, 3 /* thread count */, true /* verbose */);

        LatencyStats.setDefaultPauseDetector(pauseDetector);

        LatencyStats latencyStats = new LatencyStats();

        pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + 115 * MSEC);

        TimeServices.moveTimeForward(5000L);
        TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
        TimeServices.moveTimeForward(1000000L);
        TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
        TimeServices.moveTimeForward(2000000L);
        TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
        TimeServices.moveTimeForward(110000000L);
        TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

        try {

            TimeUnit.NANOSECONDS.sleep(10 * MSEC); // Make sure things have some time to propagate

            long startTime = TimeServices.nanoTime();
            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start: startTime = " + startTime);

            long lastTime = startTime;
            for (int i = 0 ; i < 2000; i++) {
                pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (4 * MSEC));
                TimeServices.moveTimeForwardMsec(5);
//                TimeUnit.NANOSECONDS.sleep(100000L); // Give things have some time to propagate
                long now = TimeServices.nanoTime();
                latencyStats.recordLatency(now - lastTime);
                lastTime = now;
            }

            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 10 sec from start
            System.out.println("\nForcing Interval Update:\n");
            latencyStats.forceIntervalSample();

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(lastTime));

            Histogram accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            Histogram intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2000, accumulatedHistogram.getHistogramData().getTotalCount());

            System.out.println("Pausing detector threads for 5 seconds:");
            pauseDetector.stallDetectorThreads(0x7, 5000 * MSEC);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 15 sec from start

            // Report without forcing interval measurement update:

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause, pre-observation Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause, pre-observation Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2000, accumulatedHistogram.getHistogramData().getTotalCount());

            // Still @ 15 sec from start
            System.out.println("\nForcing Interval Update:\n");
            latencyStats.forceIntervalSample();

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause, post-observation Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause, post-observation Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2998, accumulatedHistogram.getHistogramData().getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * MSEC));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 15.5 sec from start
            System.out.println("\nForcing Interval Update:\n");
            latencyStats.forceIntervalSample();

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2998, accumulatedHistogram.getHistogramData().getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * MSEC));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 16 sec from start
            System.out.println("\nForcing Interval Update:\n");
            latencyStats.forceIntervalSample();

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2999", 2998, accumulatedHistogram.getHistogramData().getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (2000 * MSEC));
            TimeServices.moveTimeForwardMsec(2000);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 18 sec from start
            System.out.println("\nForcing Interval Update:\n");
            latencyStats.forceIntervalSample();

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2999", 2998, accumulatedHistogram.getHistogramData().getTotalCount());

            for (int i = 0 ; i < 100; i++) {
                pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (5 * MSEC));
                TimeServices.moveTimeForwardMsec(5);
                long now = TimeServices.nanoTime();
                latencyStats.recordLatency(now - lastTime);
                lastTime = now;
            }

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * MSEC));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 19 sec from start
            System.out.println("\nForcing Interval Update:\n");
            latencyStats.forceIntervalSample();


            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();
            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());


            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * MSEC));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 19.5 sec from start
            System.out.println("\nForcing Interval Update:\n");
            latencyStats.forceIntervalSample();

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " + latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            accumulatedHistogram = latencyStats.getAccumulatedHistogram();
            intervalHistogram = latencyStats.getIntervalHistogram();
            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " + accumulatedHistogram.getHistogramData().getMean() + ", count = " + accumulatedHistogram.getHistogramData().getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " + intervalHistogram.getHistogramData().getMean() + ", count = " + intervalHistogram.getHistogramData().getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 3098, accumulatedHistogram.getHistogramData().getTotalCount());
        } catch (InterruptedException ex) {

        }

        latencyStats.stop();

        pauseDetector.shutdown();
    }

}

