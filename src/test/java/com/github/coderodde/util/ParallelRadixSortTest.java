package com.github.coderodde.util;

import java.util.Arrays;
import java.util.Random;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public final class ParallelRadixSortTest {
    
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
        
//        final int ARRAY_SIZE = 2763;
//        final int FROM_INDEX = 172;
//        final int TO_INDEX = 2513;
        
        final int ARRAY_SIZE = 23;  // 40
        final int FROM_INDEX = 1;   // 3
        final int TO_INDEX = ARRAY_SIZE - 2; // - 3
        
        int[] array1 = Utils.createRandomIntArray(ARRAY_SIZE, random);
        int[] array2 = array1.clone();
        
        Arrays.sort(array1, FROM_INDEX, TO_INDEX);
        
        ParallelRadixSort.parallelSort(
                array2, 
                FROM_INDEX, 
                TO_INDEX);
        
        assertTrue(Arrays.equals(array1, array2));
   }
}
