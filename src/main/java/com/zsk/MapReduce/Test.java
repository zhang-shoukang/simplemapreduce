package com.zsk.MapReduce;

import java.util.HashMap;
import java.util.Scanner;

public class Test {
    public static void main(String[] args) {
        int[] arr = {9,36,86,11,99,72,25,13,19,26};
       int[] arr_v = {0,1,1,1,1,1,1,1,1,1};
       int j=0;
       for (int i=1;i<arr[0];i++){
            if (arr[i]<arr[i+1]){
                while (arr_v[i]>=arr_v[i+1])
                    arr_v[i+1]=1+arr_v[i+1];
            }else if (arr[i]>arr[i+1]){
                while (arr_v[i]<=arr_v[i+1])
                    arr_v[i]=1+arr_v[i];
               j++;
            }
       }
       while (j>0){
           for (int i=1;i<arr[0];i++){
               if (arr[i]<arr[i+1]){
                   while (arr_v[i]>=arr_v[i+1])
                       arr_v[i+1]=1+arr_v[i+1];
               }else if (arr[i]>arr[i+1]){
                   while (arr_v[i]<=arr_v[i+1])
                       arr_v[i]=1+arr_v[i];
               }
           }
           j--;
       }
       int sum=0;
        for (int i : arr_v) {
            sum+=i;
        }
        System.out.println(sum);

    }

}
