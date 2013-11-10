/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;


/**
 * JUnit test for {@link org.LatencyUtils.TimeCappedMovingAverageIntervalEstimatorTest}
 */
public class TimeCappedMovingAverageIntervalEstimatorTest {

    @Test
    public void testWindowBehavior() throws Exception {
        MyArtificialPauseDetector pauseDetector = new MyArtificialPauseDetector();
        TimeCappedMovingAverageIntervalEstimator estimator = new TimeCappedMovingAverageIntervalEstimator(32, 1000000000L /* 1 sec */, pauseDetector);
        // Listeners are added as a concurrent operation, so wait a bit to let the listener register before we
        // start notifying of pauses:
        java.util.concurrent.locks.LockSupport.parkNanos(20000000L);

        long now = 0;

        for (int i = 0; i < 10000; i++) {
            estimator.recordInterval(20, now);
        }

        Assert.assertEquals("expected interval to be 20", 20, estimator.getEstimatedInterval(0));


        for (int i = 0; i < 16; i++) {
            estimator.recordInterval(40, now);
        }

        Assert.assertEquals("expected interval to be 30", 30, estimator.getEstimatedInterval(0));

        for (int i = 0; i < 8; i++) {
            estimator.recordInterval(60, 2000000000L);
        }

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(0L));

        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(2000000000L));

        pauseDetector.recordPause(1500000000L, 1500000000L);

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(0L));

        // Because of the pause, 2.0 sec should still be inside the window:

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(2000000000L));

        // 3.0 sec is outside of the window (more than 1 second past the end of the 1.5 sec pause) :

        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(3000000000L));

        // 0 is still inside the window:

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(0L));

        // Since recorded pause has been popped (by 3.0 sec get), 2.0 sec should now be outside of the window:

        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(2000000000L));

        // Record two pauses, one at 0 sec and one at 2 sec:

        pauseDetector.recordPause(1500000000L, 1500000000L);

        pauseDetector.recordPause(1500000000L /* 1.5 seconds pause */, 3500000000L /* started 2.1 seconds in */);

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(0L));

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(1000000000L));

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(2000000000L));

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(3000000000L));

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(4000000000L));

        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(4200000000L));

        // The 4.2 seconds get popped the first 1.5 second pause (that ended at 1.5 seconds), but not the second
        // (which ended at 3.5 seconds and still overlaps with the 1 second time cap), so up to 2.5 seconds
        // will still be in the window, but above that is already outside the window:

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(1000000000L));

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(2000000000L));

        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(3000000000L));


        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(6000000000L));

        // 0 is still inside the window:

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(0L));

        Assert.assertEquals("expected interval to be 40", 40, estimator.getEstimatedInterval(1000000000L));

        // Since all recorded pauses have been popped (by 6.0 sec get), 2.0 sec should now be outside of the window:

        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(2000000000L));

    }

    volatile long origListenerSize;
    @Test
    public void testRecodingWithPauseOverlap() throws Exception {
        MyArtificialPauseDetector pauseDetector = new MyArtificialPauseDetector();
        TimeCappedMovingAverageIntervalEstimator estimator = new TimeCappedMovingAverageIntervalEstimator(32, 100000000000L /* 100 sec */, pauseDetector);
        // Listeners are added as a concurrent operation, so wait a bit to let the listener register before we
        // start notifying of pauses:
        java.util.concurrent.locks.LockSupport.parkNanos(20000000L);

        Assert.assertEquals("expected interval to be MAX_VALUE", Long.MAX_VALUE, estimator.getEstimatedInterval(0));


        pauseDetector.recordPause(1500000000L, 2500000000L); // 1.5 sec pause that starts at 1 sec.

        for (int i = 0; i < 32; i++) {
            estimator.recordInterval(2500000000L, 3000000000L); // A 2.5 sec interval that ends at 3 seconds (started at 0.5 sec);
        }

        // Should have recorded a 1 second interval, not a 2.5 second one:
        Assert.assertEquals("expected interval to be 1000000000L", 1000000000L, estimator.getEstimatedInterval(3000000000L));

        for (int i = 0; i < 32; i++) {
            estimator.recordInterval(2500000000L, 6000000000L); // A 2.5 sec interval that ends at 6 seconds (started at 3.5 sec);
        }
        Assert.assertEquals("expected interval to be 2500000000L", 2500000000L, estimator.getEstimatedInterval(6000000000L));
    }



    class MyArtificialPauseDetector extends PauseDetector {
        public volatile long latestPauseEndTime = 0;
        public void recordPause(long length, long when) {
            notifyListeners(length, when);
            latestPauseEndTime = when;
        }
    }
}
