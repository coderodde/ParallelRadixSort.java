package com.github.coderodde.util;

import java.util.Random;

public final class Utils {
    
    private static final int MAX_VALUE = 1000;
    
    public static int[] createRandomIntArray(
            int size,
            int maxValue,
            Random random) {
        
        int[] a = new int[size];
        
        for (int i = 0; i < size; i++) {
            a[i] = random.nextInt(maxValue);
        }
        
        return a;
    }
    
    public static int[] createRandomIntArray(int size, Random random) {
        return createRandomIntArray(size, MAX_VALUE, random);
    }
    
    public static int[] createDebugIntArray(int size, Random random) {
        int[] array = new int[size];
        
        for (int i = 0; i != size; i++) {
            array[i] = random.nextInt(256) << 24;
        }
        
        return array;
    }
    
    public static int[] createLinearDebugIntArray(int size, Random random) {
        int[] array = new int[size];
        
        for (int i = 0; i != size; i++) {
            array[i] = i << 24;
        }
        
        for (int i = 0; i != 2 * size; i++) {
            int index1 = random.nextInt(size);
            int index2 = random.nextInt(size);
            int a = array[index1];
            array[index1] = array[index2];
            array[index2] = a;
        }
        
        return array;
    }
}
