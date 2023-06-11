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
}
