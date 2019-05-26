package com.deyu.handwriting;

/**
 * 快速排序 时间平均 O(NlogN) 最坏O( N^2 ) 空间平均O(logN)和 最坏O(N)
 * 不稳定
 */
public class QuickRem {

    public static void main(String[] args) {

        int[] data = {9, -16, 21, 23, -30, -49, 21, 30, 30};

        System.out.println ("排序之前：\n" + java.util.Arrays.toString (data));

        quickSort (data);

        System.out.println ("排序之后：\n" + java.util.Arrays.toString (data));
    }

    private static void quickSort(int[] data) {

        if (data.length != 0) {
            subSort (data, 0, data.length - 1);
        }
    }

    private static void subSort(int[] data, int start/*0*/, int end/*最后一个索引*/) {

        if (start < end) {

            int base = data[start];
            int i = start;
            int j = end + 1;//+1 +1 +1

            while (true) {

                while (i < end && data[++i] <= base)
                    ;
                while (j > start && data[--j] >= base)
                    ;
                if (i < j)
                    swap (data, i, j);
                else
                    break;
            }
            swap (data, start, j);
            subSort (data, start, j - 1);
            subSort (data, j + 1, end);
        }
    }

    private static void swap(int[] data, int i, int j) {
        int temp = data[i];
        data[i] = data[j];
        data[j] = temp;
    }
}
