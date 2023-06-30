package com.github.coderodde.util;

import java.util.Arrays;
import java.util.Random;

final class ParallelRadixSortBenchmark {
    
    private static final int BENCHMARK_ITERATIONS = 20;
    private static final int MAXIMUM_ARRAY_SIZE = 100_000_000;
    private static final int MINIMUM_ARRAY_SIZE = 90_000_000;
    private static final int MAXIMUM_FROM_INDEX = 1313;
    private static final int MAXIMUM_SKIP_LAST_ELEMENTS = 1711;
    
    public static void main(String[] args) {
        System.out.println("Warming up benchmark 1...");
        benchmark(false);
        System.out.println("Warming up benchmark 2...");
        benchmark2(false);
        System.out.println("Benchmarking 1...");
        benchmark(true);
        System.out.println("Benchmarking 2...");
        benchmark2(true);
        System.out.println("Benchmark done!");
    }
    
    private static void benchmark(boolean print) {
        Random random = new Random();
        long totalDuration1 = 0L;
        long totalDuration2 = 0L;
        
        for (int iteration = 0; iteration < BENCHMARK_ITERATIONS; iteration++) {
            
            int arrayLength = 
                    MINIMUM_ARRAY_SIZE +-
                    random.nextInt(MAXIMUM_ARRAY_SIZE - MINIMUM_ARRAY_SIZE + 1);
                            
            int[] array1 = Utils.createRandomIntArray(arrayLength, random);
            int[] array2 = array1.clone();
            
            int fromIndex = random.nextInt(MAXIMUM_FROM_INDEX + 1);
            int toIndex = array1.length -
                    random.nextInt(MAXIMUM_SKIP_LAST_ELEMENTS + 1);
            
            long startTime = System.currentTimeMillis();
            Arrays.parallelSort(array1, fromIndex, toIndex);
            long endTime = System.currentTimeMillis();
            long duration1 = endTime - startTime;
            totalDuration1 += duration1;
            
            startTime = System.currentTimeMillis();
            ParallelRadixSort.parallelSort(array2, fromIndex, toIndex);
            endTime = System.currentTimeMillis();
            long duration2 = endTime - startTime;
            totalDuration2 += duration2;
            
            boolean agreed = Arrays.equals(array1, array2);
            
            if (print) {
                System.out.println(
                        "Arrays.parallelSort: "
                                + duration1 
                                + " ms, ParallelRadixSort.parallelSort: " 
                                + duration2
                                + " ms, agreed: " 
                                + agreed);
            }
        }
        
        if (print) {
            System.out.println(
                    "Total Arrays.parallelSort duration: " 
                            + totalDuration1
                            + ", total ParallelRadixSort.parallelSort: " 
                            + totalDuration2);
        }
    }
    
    private static void benchmark2(boolean print) {
        Random random = new Random();
        long totalDuration1 = 0L;
        long totalDuration2 = 0L;
        
        for (int iteration = 0; iteration < BENCHMARK_ITERATIONS; iteration++) {
            
            int arrayLength = 
                    MINIMUM_ARRAY_SIZE +-
                    random.nextInt(MAXIMUM_ARRAY_SIZE - MINIMUM_ARRAY_SIZE + 1);
                            
            int[] array1 = new int[arrayLength];
            int[] array2 = array1.clone();
            
            int fromIndex = random.nextInt(MAXIMUM_FROM_INDEX + 1);
            int toIndex = array1.length -
                    random.nextInt(MAXIMUM_SKIP_LAST_ELEMENTS + 1);
            
            long startTime = System.currentTimeMillis();
            Arrays.parallelSort(array1, fromIndex, toIndex);
            long endTime = System.currentTimeMillis();
            long duration1 = endTime - startTime;
            totalDuration1 += duration1;
            
            startTime = System.currentTimeMillis();
            ParallelRadixSort.parallelSort(array2, fromIndex, toIndex);
            endTime = System.currentTimeMillis();
            long duration2 = endTime - startTime;
            totalDuration2 += duration2;
            
            boolean agreed = Arrays.equals(array1, array2);
            
            if (print) {
                System.out.println(
                        "Arrays.parallelSort: "
                                + duration1 
                                + " ms, ParallelRadixSort.parallelSort: " 
                                + duration2
                                + " ms, agreed: " 
                                + agreed);
            }
        }
        
        if (print) {
            System.out.println(
                    "Total Arrays.parallelSort duration: " 
                            + totalDuration1
                            + ", total ParallelRadixSort.parallelSort: " 
                            + totalDuration2);
        }
    }
}
