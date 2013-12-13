/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.Comparator;
import java.util.TimerTask;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Provide an API for time-related service, such as getting the current time and waiting for
 * a given period of time. By default, these services are provided by actual time services
 * in the JDK (i.e. System.nanoTime(), System.currentTimeMillis(), Thread.sleep(), and
 * java.util.concurrent.locks.LockSupport.parkNanos()). However, if the property
 * LatencyUtils.useActualTime is set to "false", TimeServers will only move the notion
 * of time in response to calls to the #setCurrentTime() method.
 *
 */
public class TimeServices {
    static final boolean useActualTime;


    static long currentTime;

    // Use a monitor to notify any waiters that time has changed.
    // TODO: if we find this to be inefficient for some reason, replace with a timer wheel or some such.
    final static Object timeUpdateMonitor = new Object();

    static {
        String useActualTimeProperty = System.getProperty("LatencyUtils.useActualTime", "true");
        useActualTime = !useActualTimeProperty.equals("false");
    }

    public static long nanoTime() {
        if (useActualTime) {
            return System.nanoTime();
        }
        return currentTime;
    }

    public static long currentTimeMillis() {
        if (useActualTime) {
            return System.currentTimeMillis();
        }
        return currentTime/ 1000000;
    }

    public static void sleepMsecs(long sleepTimeMsec) throws InterruptedException {
        if (useActualTime) {
            Thread.sleep(sleepTimeMsec);
        }
        waitUntilTime(currentTime + (sleepTimeMsec * 1000000L));
    }

    public static void sleepNanos(long sleepTimeNsec) throws InterruptedException {
        if (useActualTime) {
            java.util.concurrent.locks.LockSupport.parkNanos(sleepTimeNsec);
        }
        waitUntilTime(currentTime + (sleepTimeNsec));
    }

    static void waitUntilTime(long timeToWakeAt) throws InterruptedException {
        synchronized (timeUpdateMonitor) {
            while (timeToWakeAt > currentTime) {
                timeUpdateMonitor.wait();
            }
        }
    }

    public void setCurrentTime(long currentTime) {
        if (useActualTime) {
            throw new IllegalStateException("Can't set current time when (useActualTime != false)");
        }
        TimeServices.currentTime = currentTime;
        synchronized (timeUpdateMonitor) {
            timeUpdateMonitor.notifyAll();
        }
    }

    public static class Timer {
        final java.util.Timer actualTimer;
        final Thread internalThread;
        PriorityBlockingQueue<TimerTaskEntry> taskEntries = new PriorityBlockingQueue<TimerTaskEntry>(10000, compareTimerTaskEntryByStartTime);

        public Timer() {
            this(null, false);
        }

        public Timer(String name) {
            this(name, false);
        }

        public Timer(String name, boolean isDaemon) {
            if (useActualTime) {
                if (name != null) {
                    actualTimer = new java.util.Timer(name, isDaemon);
                } else {
                    actualTimer = new java.util.Timer(isDaemon);
                }
                internalThread = null;
                return;
            }

            actualTimer = null;
            if (name != null) {
                internalThread = new Thread(name);
            } else {
                internalThread = new Thread();
            }
            internalThread.setDaemon(isDaemon);
        }

        public void scheduleAtFixedRate(TimerTask timerTask, long delay, long period) {
            if (useActualTime) {
                actualTimer.scheduleAtFixedRate(timerTask, delay, period);
                return;
            }
                TimerTaskEntry entry = new TimerTaskEntry(currentTime + delay, timerTask, period, true);
            synchronized (timeUpdateMonitor) {
                taskEntries.add(entry);
                notifyAll();
            }
        }


        private class TimerThread extends Thread {
            public void run() {
                try {
                    while (true) {
                        synchronized (timeUpdateMonitor) {
                            while (taskEntries.peek().getStartTime() < currentTime) {
                                TimerTaskEntry entry = taskEntries.poll();
                                entry.getTimerTask().run();
                                if (entry.shouldReschedule()) {
                                    entry.setNewStartTime(currentTime);
                                    taskEntries.add(entry);
                                }
                            }
                            timeUpdateMonitor.wait();
                        }
                    }
                } catch (InterruptedException ex) {

                }
            }
        }

    }

    private static class TimerTaskEntry {
        long startTime;
        TimerTask timerTask;
        long period;
        long initialStartTime;
        long executionCount;
        boolean fixedRate;

        TimerTaskEntry(long startTime, TimerTask timerTask, long period, boolean fixedRate) {
            this.initialStartTime = startTime;
            this.startTime = startTime;
            this.timerTask = timerTask;
            this.period = period;
            this.fixedRate = fixedRate;
        }

        boolean shouldReschedule() {
            return (period != 0);
        }

        public long getStartTime() {
            return startTime;
        }

        public TimerTask getTimerTask() {
            return timerTask;
        }

        public void setNewStartTime(long timeNow) {
            if (period == 0) {
                throw new IllegalStateException("should nto try to reschedule an entry that has no interval or rare");
            }
            if (!fixedRate) {
                startTime = timeNow + period;
            } else {
                executionCount++;
                startTime = initialStartTime + (executionCount * period);
            }

        }
    }

    private static CompareTimerTaskEntryByStartTime compareTimerTaskEntryByStartTime = new CompareTimerTaskEntryByStartTime();

    static class CompareTimerTaskEntryByStartTime implements Comparator<TimerTaskEntry> {
        public int compare(TimerTaskEntry r1, TimerTaskEntry r2) {
            long t1 = r1.startTime;
            long t2 = r2.startTime;
            return (t1 > t2) ? 1 : ((t1 < t2) ? -1 : 0);
        }
    }
}
