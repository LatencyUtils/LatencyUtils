/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.Comparator;
import java.util.concurrent.*;

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

    public static void sleepMsecs(long sleepTimeMsec) {
        try {
            if (useActualTime) {
                Thread.sleep(sleepTimeMsec);
            } else {
                waitUntilTime(currentTime + (sleepTimeMsec * 1000000L));
            }
        } catch (InterruptedException ex) {
        }
    }

    public static void sleepNanos(long sleepTimeNsec) {
        try {
            if (useActualTime) {
                TimeUnit.NANOSECONDS.sleep(sleepTimeNsec);
            } else {
                waitUntilTime(currentTime + (sleepTimeNsec));
            }
        } catch (InterruptedException ex) {
        }
    }

    public static void waitUntilTime(long timeToWakeAt) throws InterruptedException {
        synchronized (timeUpdateMonitor) {
            while (timeToWakeAt > currentTime) {
                timeUpdateMonitor.wait();
            }
        }
    }

    public static void moveTimeForward(long timeDeltaNsec) throws InterruptedException {
        setCurrentTime(currentTime + timeDeltaNsec);
    }

    public static void moveTimeForwardMsec(long timeDeltaMsec) throws InterruptedException {
        moveTimeForward(timeDeltaMsec * 1000000L);
    }

    public static void setCurrentTime(long newCurrentTime) throws InterruptedException {
        if (newCurrentTime < nanoTime()) {
            throw new IllegalStateException("Can't set current time to the past.");
        }
        if (useActualTime) {
            while (newCurrentTime > nanoTime()) {
                TimeUnit.NANOSECONDS.sleep(newCurrentTime - nanoTime());
            }
            return;
        }
        while (currentTime < newCurrentTime) {
            long timeDelta = Math.min((newCurrentTime - currentTime), 5000000L);
            currentTime += timeDelta;
            synchronized (timeUpdateMonitor) {
                timeUpdateMonitor.notifyAll();
                TimeUnit.NANOSECONDS.sleep(50000);
            }
        }

    }

    public static class ScheduledExecutor {
        private final ScheduledThreadPoolExecutor actualExecutor;

        final MyExecutorThread internalExecutorThread;
        final PriorityBlockingQueue<RunnableTaskEntry> taskEntries;

        ScheduledExecutor() {
            if (useActualTime) {
                actualExecutor = new ScheduledThreadPoolExecutor(1);
                internalExecutorThread = null;
                taskEntries = null;
            } else {
                actualExecutor = null;
                taskEntries = new PriorityBlockingQueue<RunnableTaskEntry>(10000, compareRunnableTaskEntryByStartTime);
                internalExecutorThread = new MyExecutorThread();
                internalExecutorThread.setDaemon(true);
                internalExecutorThread.start();
            }
        }

        public void scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            if (useActualTime) {
                actualExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
                return;
            }

            long startTimeNsec = currentTime + TimeUnit.NANOSECONDS.convert(initialDelay, unit);
            long periodNsec = TimeUnit.NANOSECONDS.convert(period, unit);
            RunnableTaskEntry entry = new RunnableTaskEntry(command, startTimeNsec, periodNsec, true /* fixed rate */);
            synchronized (timeUpdateMonitor) {
                taskEntries.add(entry);
                timeUpdateMonitor.notifyAll();
            }
        }

        public void shutdown() {
            if (useActualTime) {
                actualExecutor.shutdownNow();
                return;
            }
            internalExecutorThread.terminate();
        }

        private class MyExecutorThread extends Thread {
            volatile boolean doRun = true;

            void terminate() {
                synchronized (timeUpdateMonitor) {
                    doRun = false;
                    timeUpdateMonitor.notifyAll();
                }
            }

            public void run() {
                try {
                    while (doRun) {
                        synchronized (timeUpdateMonitor) {
                            for (RunnableTaskEntry entry = taskEntries.peek();
                                 ((entry != null) && (entry.getStartTime() < currentTime));
                                 entry = taskEntries.peek()) {
                                entry.getCommand().run();
                                if (entry.shouldReschedule()) {
                                    entry.setNewStartTime(currentTime);
                                    taskEntries.add(entry);
                                }
                            }
                            timeUpdateMonitor.wait();
                        }
                    }
                } catch (InterruptedException ex) {
                } catch (CancellationException ex) {
                }
            }
        }

        private static class RunnableTaskEntry {
            long startTime;
            Runnable command;
            long period;
            long initialStartTime;
            long executionCount;
            boolean fixedRate;

            RunnableTaskEntry(Runnable command, long startTimeNsec, long periodNsec, boolean fixedRate) {
                this.command = command;
                this.startTime = startTimeNsec;
                this.initialStartTime = startTimeNsec;
                this.period = periodNsec;
                this.fixedRate = fixedRate;
            }

            boolean shouldReschedule() {
                return (period != 0);
            }

            public long getStartTime() {
                return startTime;
            }

            public Runnable getCommand() {
                return command;
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

        private static CompareRunnableTaskEntryByStartTime compareRunnableTaskEntryByStartTime = new CompareRunnableTaskEntryByStartTime();

        static class CompareRunnableTaskEntryByStartTime implements Comparator<RunnableTaskEntry> {
            public int compare(RunnableTaskEntry r1, RunnableTaskEntry r2) {
                long t1 = r1.startTime;
                long t2 = r2.startTime;
                return (t1 > t2) ? 1 : ((t1 < t2) ? -1 : 0);
            }
        }
    }
}
