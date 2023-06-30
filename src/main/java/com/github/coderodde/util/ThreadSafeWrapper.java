package com.github.coderodde.util;

import java.util.concurrent.Semaphore;

/**
 * This class provides the facilities for sorting and setting the thresholds in
 * multithreaded environments.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Jun 30, 2023)
 */
public final class ThreadSafeWrapper {
    
    private static final Semaphore MUTEX = new Semaphore(1, true);
    
    public static void setInsertionsortThreshold(
            int newInsertionsortThreshold) {
        
        MUTEX.acquireUninterruptibly();
        
        ParallelRadixSort.setInsertionSortThreshold(
                newInsertionsortThreshold);
        
        MUTEX.release();
    }
    
    public static void setMergesortThreshold(
            int newMergesortThreshold) {
        
        MUTEX.acquireUninterruptibly();
        
        ParallelRadixSort.setMergesortThreshold(
                newMergesortThreshold);
        
        MUTEX.release();
    }
    
    public static void setThreadWorkloadThreshold(
            int newThreadWorkloadThreshold) {
        
        MUTEX.acquireUninterruptibly();
        
        ParallelRadixSort.setMinimumThreadWorkload(
                newThreadWorkloadThreshold);
        
        MUTEX.release();
    }
    
    public static void parallelSort(int[] array) {
        MUTEX.acquireUninterruptibly();
        
        ParallelRadixSort.parallelSort(array);
        
        MUTEX.release();
    }
    
    public static void parallelSort(int[] array, int fromIndex, int toIndex) {
        MUTEX.acquireUninterruptibly();
        
        ParallelRadixSort.parallelSort(array, fromIndex, toIndex);
        
        MUTEX.release();
    }
}
