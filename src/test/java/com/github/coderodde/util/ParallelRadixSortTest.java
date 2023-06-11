package com.github.coderodde.util;

import java.util.Arrays;
import java.util.Random;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;

public final class ParallelRadixSortTest {
    
    @BeforeClass
    public static void setupBeforeClass() {
        ParallelRadixSort.setInsertionSortThreshold(-1);
        ParallelRadixSort.setMergesortThreshold(-1);
    }
    
    @Test
    public void testInsertionSort() {
        Random random = new Random(13L);
        
        final int ARRAY_SIZE = 13;
        final int FROM_INDEX = 1;
        final int TO_INDEX = ARRAY_SIZE - 3; // Ignore 3 last elements.
        
        int[] array1 = Utils.createRandomIntArray(ARRAY_SIZE, random);
        int[] array2 = array1.clone();
        
        Arrays.sort(array1, FROM_INDEX, TO_INDEX);
        ParallelRadixSort.parallelSort(array2, FROM_INDEX, TO_INDEX);
        
        assertTrue(Arrays.equals(array1, array2));
    }
        
    @Test
    public void testMergesort() {
        Random random = new Random(123L);
        
        final int ARRAY_SIZE = 50;
        final int FROM_INDEX = 13;
        final int TO_INDEX = ARRAY_SIZE - 13;
        
        int[] array1 = Utils.createRandomIntArray(ARRAY_SIZE, random);
        int[] array2 = array1.clone();
        
        Arrays.sort(array1, FROM_INDEX, TO_INDEX);
        
        ParallelRadixSort.parallelSort(
                array2, 
                FROM_INDEX, 
                TO_INDEX);
        
        assertTrue(Arrays.equals(array1, array2));
   }
    
    @Test
    public void testSerialRadixSort() {
        Random random = new Random(26);
        final int SIZE = 128;
        int[] array1 = Utils.createRandomIntArray(
                SIZE, 
                Integer.MAX_VALUE - 1,
                random);
        
        int[] array2 = array1.clone();
        
        final int FROM_INDEX = 14;
        final int TO_INDEX = SIZE - 14;
        
        Arrays.sort(array1, FROM_INDEX, TO_INDEX);
        ParallelRadixSort.parallelSort(
                array2, 
                FROM_INDEX,
                TO_INDEX);
        
        assertTrue(Arrays.equals(array1, array2));
    }
    
   @Test
   public void bruteForceTestInsertionsort() {
       final int ITERATIONS = 200;
       Random random = new Random(432);
       
       for (int iteration = 0; iteration < ITERATIONS; iteration++) {
           int arrayLength = 
                   random.nextInt(
                           ParallelRadixSort.DEFAULT_INSERTION_SORT_THRESHOLD + 1);
           
           int[] array1 = Utils.createRandomIntArray(arrayLength, random);
           int[] array2 = array1.clone();
           int cutFromFront = random.nextInt(arrayLength + 1);
           
           if (cutFromFront == arrayLength) {
                Arrays.sort(array1, arrayLength, arrayLength);
                        
                ParallelRadixSort.parallelSort(
                        array2, 
                        arrayLength, 
                        arrayLength);
                
                assertTrue(Arrays.equals(array1, array2));
                continue;
           }
           
           int cutFromBack = random.nextInt(arrayLength - cutFromFront + 1);
           
           if (cutFromBack + cutFromFront < arrayLength) {
               int fromIndex = cutFromFront;
               int toIndex = arrayLength - cutFromBack;
               
               Arrays.sort(array1, fromIndex, toIndex);
               ParallelRadixSort.parallelSort(array2, fromIndex, toIndex);
               
               assertTrue(Arrays.equals(array1, array2));
           } else {
               // Once here, we could not generate a non-empty array. Repeat the
               // iteration:
               iteration--;
               continue;
           }
       }
   }
    
   @Test
   public void bruteForceTestMergesort() {
       final int ITERATIONS = 200;
       Random random = new Random(3);
       
       for (int iteration = 0; iteration < ITERATIONS; iteration++) {
           int arrayLength = 
                   random.nextInt(
                           ParallelRadixSort.DEFAULT_MERGESORT_THRESHOLD + 1);
           
           int[] array1 = Utils.createRandomIntArray(arrayLength, random);
           int[] array2 = array1.clone();
           int cutFromFront = random.nextInt(arrayLength + 1);
           
           if (cutFromFront == arrayLength) {
                Arrays.sort(array1, arrayLength, arrayLength);
                        
                ParallelRadixSort.parallelSort(
                        array2, 
                        arrayLength, 
                        arrayLength);
                
                assertTrue(Arrays.equals(array1, array2));
                continue;
           }
           
           int cutFromBack = random.nextInt(arrayLength - cutFromFront + 1);
           
           if (cutFromBack + cutFromFront < arrayLength) {
               int fromIndex = cutFromFront;
               int toIndex = arrayLength - cutFromBack;
               
               Arrays.sort(array1, fromIndex, toIndex);
               ParallelRadixSort.parallelSort(array2, fromIndex, toIndex);
               
               assertTrue(Arrays.equals(array1, array2));
           } else {
               // Once here, we could not generate a non-empty array. Repeat the
               // iteration:
               iteration--;
               continue;
           }
       }
   }
   
   @Test
   public void getBucketIndex() {
       int bucketKey =
               ParallelRadixSort.getBucketIndex(
                       0x12345678, 
                       0);
       
       assertEquals(146, bucketKey);
       
       bucketKey =
               ParallelRadixSort.getBucketIndex(
                       0x12345678, 
                       1);
       
       assertEquals(0x34, bucketKey);
       
       bucketKey =
               ParallelRadixSort.getBucketIndex(
                       0x12345678, 
                       2);
       
       assertEquals(0x56, bucketKey);
       
       bucketKey =
               ParallelRadixSort.getBucketIndex(
                       0x12345678, 
                       3);
       
       assertEquals(0x78, bucketKey);
   }
}
