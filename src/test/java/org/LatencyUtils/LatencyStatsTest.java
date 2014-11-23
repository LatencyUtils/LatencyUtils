/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


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

        Histogram accumulatedHistogram = new Histogram(latencyStats.getIntervalHistogram());

        try {
            Thread.sleep(100);

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + 115 * MSEC);

            TimeServices.moveTimeForward(5000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
            TimeServices.moveTimeForward(1000000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
            TimeServices.moveTimeForward(2000000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
            TimeServices.moveTimeForward(110000000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate


            TimeUnit.NANOSECONDS.sleep(10 * MSEC); // Make sure things have some time to propagate

            long startTime = TimeServices.nanoTime();
            System.out.println("@ " + (TimeServices.nanoTime() - startTime) +
                    " nsec after start: startTime = " + startTime);

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
            Histogram intervalHistogram = latencyStats.getIntervalHistogram();
            accumulatedHistogram.add(intervalHistogram);

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(lastTime));


            System.out.println("Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2000, accumulatedHistogram.getTotalCount());

            System.out.println("Pausing detector threads for 5 seconds:");
            pauseDetector.stallDetectorThreads(0x7, 5000 * MSEC);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 15 sec from start

            // Report without doing an interval measurement update:

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));

            System.out.println("Post-pause, pre-observation Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Post-pause, pre-observation Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2000, accumulatedHistogram.getTotalCount());

            // Still @ 15 sec from start
            System.out.println("\nForcing Interval Update:\n");

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            intervalHistogram = latencyStats.getIntervalHistogram();
            accumulatedHistogram.add(intervalHistogram);

            System.out.println("Post-pause, post-observation Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Post-pause, post-observation Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2998, accumulatedHistogram.getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * MSEC));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 15.5 sec from start
            System.out.println("\nForcing Interval Update:\n");

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            intervalHistogram = latencyStats.getIntervalHistogram();
            accumulatedHistogram.add(intervalHistogram);

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 2998, accumulatedHistogram.getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * MSEC));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 16 sec from start
            System.out.println("\nForcing Interval Update:\n");

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            intervalHistogram = latencyStats.getIntervalHistogram();
            accumulatedHistogram.add(intervalHistogram);

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2999", 2998, accumulatedHistogram.getTotalCount());

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (2000 * MSEC));
            TimeServices.moveTimeForwardMsec(2000);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 18 sec from start
            System.out.println("\nForcing Interval Update:\n");

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            intervalHistogram = latencyStats.getIntervalHistogram();
            accumulatedHistogram.add(intervalHistogram);

            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2999", 2998, accumulatedHistogram.getTotalCount());

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

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            intervalHistogram = latencyStats.getIntervalHistogram();
            accumulatedHistogram.add(intervalHistogram);
            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());


            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + (500 * MSEC));
            TimeServices.moveTimeForwardMsec(500);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            // @ 19.5 sec from start
            System.out.println("\nForcing Interval Update:\n");

            System.out.println("@ " + (TimeServices.nanoTime() - startTime) + " nsec after start:");
            System.out.println("Estimated interval = " +
                    latencyStats.getIntervalEstimator().getEstimatedInterval(TimeServices.nanoTime()));
            intervalHistogram = latencyStats.getIntervalHistogram();
            accumulatedHistogram.add(intervalHistogram);
            System.out.println("Post-pause Accumulated Average latency for 5msec sleeps: " +
                    accumulatedHistogram.getMean() + ", count = " + accumulatedHistogram.getTotalCount());
            System.out.println("Post-pause Interval Average latency for 5msec sleeps: " +
                    intervalHistogram.getMean() + ", count = " + intervalHistogram.getTotalCount());

            Assert.assertEquals("Accumulated total count should be 2000", 3098, accumulatedHistogram.getTotalCount());
        } catch (InterruptedException ex) {

        }

        latencyStats.stop();

        pauseDetector.shutdown();
    }

    @Test
    public void testIntervalSampleDeadlock() throws Exception {

        SimplePauseDetector pauseDetector = new SimplePauseDetector(1000000L /* 1 msec sleep */,
                10000000L /* 10 msec reporting threshold */, 3 /* thread count */, true /* verbose */);

        LatencyStats.setDefaultPauseDetector(pauseDetector);

        final LatencyStats latencyStats = new LatencyStats();

        try {
            Thread.sleep(100);

            pauseDetector.skipConsensusTimeTo(TimeServices.nanoTime() + 115 * MSEC);

            TimeServices.moveTimeForward(5000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
            TimeServices.moveTimeForward(1000000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
            TimeServices.moveTimeForward(2000000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate
            TimeServices.moveTimeForward(110000000L);
            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            TimeUnit.NANOSECONDS.sleep(10 * MSEC); // Make sure things have some time to propagate

            long startTime = TimeServices.nanoTime();
            try {
                latencyStats.recordLatency(Long.MAX_VALUE);
            } catch (java.lang.IndexOutOfBoundsException e) {
                //Suppress, because this is what we expect
            }

            TimeUnit.NANOSECONDS.sleep(1 * MSEC); // Make sure things have some time to propagate

            ExecutorService executorService = Executors.newFixedThreadPool(1);
            Future<Boolean> future = executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("\nForcing Interval Update; may hang forever:\n");
                        latencyStats.getIntervalHistogram();
                        System.out.println("\nCompleted forcing interval sample\n");
                    }
                },
                Boolean.TRUE
            );

            try {
                Boolean response = future.get(5, TimeUnit.SECONDS);
                Assert.assertEquals(Boolean.TRUE, response);
            } catch (TimeoutException e) {
                System.err.println("\nFuture timed out.\n");
                System.out.println("\nMaking sure the thread dies.\n");
                try {
                    Field f = LatencyStats.class.getDeclaredField("recordingEndEpoch");
                    f.setAccessible(true);
                    f.set(latencyStats, (Long)f.get(latencyStats) + 1);
                    System.out.println(":" + future.get(5, TimeUnit.SECONDS));
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                Assert.fail("Timed out trying to force interval sample.");
            } finally {
                executorService.shutdownNow();
                System.out.println("\nSuccessfully forced interval sample to complete on test failure.\n");
            }

        } catch (InterruptedException ex) {
            //Suppress
        } finally {
            latencyStats.stop();
            pauseDetector.shutdown();
        }
    }

}

