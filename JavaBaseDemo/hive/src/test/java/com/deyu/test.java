package com.deyu;

public class test {
    public static void main(String[] args) {
        int fun = test.fun(8);
        System.out.println("fun:" + fun);
    }

    public static int fun(int n) {
        System.out.println(n);
        if (n > 0) {
            return fun(n >> 1) + fun(n >> 2);
        }
        return 1;
    }
}
