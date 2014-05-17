/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.LatencyUtils;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A PauseDetector detects pauses and reports them to registered listeners
 */
public abstract class PauseDetector extends Thread {

    ArrayList<PauseDetectorListener> highPriorityListeners = new ArrayList<PauseDetectorListener>(32);
    ArrayList<PauseDetectorListener> normalPriorityListeners = new ArrayList<PauseDetectorListener>(32);

    private LinkedBlockingQueue<Object> messages = new LinkedBlockingQueue<Object>();

    volatile boolean stop = false;

    PauseDetector() {
        this.start();
    }

    /**
     * Notify listeners about a pause
     * @param pauseLengthNsec pause length (in nanoseconds)
     * @param pauseEndTimeNsec pause end time (in nanoTime)
     */
    protected synchronized void notifyListeners(final long pauseLengthNsec, final long pauseEndTimeNsec) {
        messages.add(new PauseNotification(pauseLengthNsec, pauseEndTimeNsec));
    }

    /**
     * Add a {@link org.LatencyUtils.PauseDetectorListener} listener to be notified when pauses are detected.
     * Listener will be added to the normal priority listeners list.
     * @param listener Listener to add
     */
    public synchronized void addListener(PauseDetectorListener listener) {
        addListener(listener, false);
    }

    /**
     * Add a {@link org.LatencyUtils.PauseDetectorListener} listener to be notified when pauses are detected
     * Listener will be added to either the normal priority or high priority listeners list,
     * @param listener Listener to add
     * @param isHighPriority If true, listener will be added to high priority list. If false, listener will
     *                     be added to the normal priority list.
     */
    public synchronized void addListener(PauseDetectorListener listener, boolean isHighPriority) {
        messages.add(new ChangeListenersRequest(
                (isHighPriority ?
                        ChangeListenersRequest.ChangeCommand.ADD_HIGH_PRIORITY :
                        ChangeListenersRequest.ChangeCommand.ADD_NORMAL_PRIORITY),
                listener));
    }

    /**
     * Remove a {@link org.LatencyUtils.PauseDetectorListener}
     * @param listener Listener to remove
     */
    public synchronized void removeListener(PauseDetectorListener listener) {
        messages.add(new ChangeListenersRequest(ChangeListenersRequest.ChangeCommand.REMOVE, listener));
    }

    /**
     * Stop execution of this pause detector
     */
    public void shutdown() {
        stop = true;
        this.interrupt();
    }

    /**
     * @inheritDoc
     */
    public void run() {
        while (!stop) {
            try {
                Object message = messages.take();

                if (message instanceof ChangeListenersRequest) {
                    final ChangeListenersRequest changeRequest = (ChangeListenersRequest) message;
                    if (changeRequest.command == ChangeListenersRequest.ChangeCommand.ADD_HIGH_PRIORITY) {
                        highPriorityListeners.add(changeRequest.listener);
                    } else if (changeRequest.command == ChangeListenersRequest.ChangeCommand.ADD_NORMAL_PRIORITY) {
                        normalPriorityListeners.add(changeRequest.listener);
                    } else {
                        normalPriorityListeners.remove(changeRequest.listener);
                        highPriorityListeners.remove(changeRequest.listener);
                    }

                } else if (message instanceof PauseNotification) {
                    final PauseNotification pauseNotification = (PauseNotification) message;

                    for (PauseDetectorListener listener : highPriorityListeners) {
                        listener.handlePauseEvent(pauseNotification.pauseLengthNsec, pauseNotification.pauseEndTimeNsec);
                    }

                    for (PauseDetectorListener listener : normalPriorityListeners) {
                        listener.handlePauseEvent(pauseNotification.pauseLengthNsec, pauseNotification.pauseEndTimeNsec);
                    }
                } else {
                    throw new RuntimeException("Unexpected message type received: " + message);
                }
            } catch (InterruptedException ex) {
            }
        }
    }

    static class ChangeListenersRequest {
        static enum ChangeCommand {ADD_HIGH_PRIORITY, ADD_NORMAL_PRIORITY, REMOVE};

        final ChangeCommand command;
        final PauseDetectorListener listener;

        ChangeListenersRequest(final ChangeCommand changeCommand,
                               final PauseDetectorListener listener) {
            this.command = changeCommand;
            this.listener = listener;
        }
    }

    static class PauseNotification {
        final long pauseLengthNsec;
        final long pauseEndTimeNsec;

        PauseNotification(final long pauseLengthNsec, final long pauseEndTimeNsec) {
            this.pauseLengthNsec = pauseLengthNsec;
            this.pauseEndTimeNsec = pauseEndTimeNsec;
        }
    }


}
