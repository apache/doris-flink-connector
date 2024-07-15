package org.apache.doris.flink.util;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * idea for this util is from https://bugs.openjdk.org/browse/JDK-8144808 991ms :
 * LocalDateTime.parse(...) 246ms : LocalDateTime.of(...)
 */
public final class FastDateUtil {

    public static LocalDateTime fastParseDateTime(String dateTime, String pattern) {
        char[] arr = dateTime.toCharArray();
        int[] indexes =
                new int[] {
                    pattern.indexOf("yyyy"),
                    pattern.indexOf("MM"),
                    pattern.indexOf("dd"),
                    pattern.indexOf("HH"),
                    pattern.indexOf("mm"),
                    pattern.indexOf("ss"),
                    pattern.indexOf("SSSSSS")
                };
        int year = parseFromIndex(arr, indexes[0], indexes[0] + 4);
        int month = parseFromIndex(arr, indexes[1], indexes[1] + 2);
        int day = parseFromIndex(arr, indexes[2], indexes[2] + 2);
        int hour = parseFromIndex(arr, indexes[3], indexes[3] + 2);
        int minute = parseFromIndex(arr, indexes[4], indexes[4] + 2);
        int second = parseFromIndex(arr, indexes[5], indexes[5] + 2);
        int nanos = parseFromIndex(arr, indexes[6], indexes[6] + 6) * 100;
        return LocalDateTime.of(year, month, day, hour, minute, second, nanos);
    }

    public static LocalDate fastParseDate(String dateTime, String pattern) {
        char[] arr = dateTime.toCharArray();
        int[] indexes =
                new int[] {
                    pattern.indexOf("yyyy"), pattern.indexOf("MM"), pattern.indexOf("dd"),
                };
        int year = parseFromIndex(arr, indexes[0], indexes[0] + 4);
        int month = parseFromIndex(arr, indexes[1], indexes[1] + 2);
        int day = parseFromIndex(arr, indexes[2], indexes[2] + 2);
        return LocalDate.of(year, month, day);
    }

    private static int parseFromIndex(char[] arr, int start, int end) {
        int value = 0;
        for (int i = start; i < end; i++) {
            value = value * 10 + (arr[i] - '0');
        }
        return value;
    }
}
