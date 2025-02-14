// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

public class IPUtils {
    /**
     * Create an IPv6 address from a (positive) {@link java.math.BigInteger}. The magnitude of the
     * {@link java.math.BigInteger} represents the IPv6 address value. Or in other words, the {@link
     * java.math.BigInteger} with value N defines the Nth possible IPv6 address.
     *
     * @param bigInteger {@link java.math.BigInteger} value
     * @return IPv6 address
     */
    public static String fromBigInteger(BigInteger bigInteger) {
        byte[] bytes = bigInteger.toByteArray();
        if (bytes[0] == 0) {
            bytes = Arrays.copyOfRange(bytes, 1, bytes.length); // Skip leading zero byte
        }
        bytes = prefixWithZeroBytes(bytes);
        long[] ipv6Bits = fromByteArray(bytes);
        return toIPv6String(ipv6Bits[0], ipv6Bits[1]);
    }

    private static byte[] prefixWithZeroBytes(byte[] original) {
        byte[] target = new byte[16];
        System.arraycopy(original, 0, target, 16 - original.length, original.length);
        return target;
    }

    /**
     * Create an IPv6 address from a byte array.
     *
     * @param bytes byte array with 16 bytes (interpreted unsigned)
     * @return IPv6 address
     */
    public static long[] fromByteArray(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            throw new IllegalArgumentException("Byte array must be exactly 16 bytes long");
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        LongBuffer longBuffer = buf.asLongBuffer();
        return new long[] {longBuffer.get(), longBuffer.get()};
    }

    private static String toShortHandNotationString(long highBits, long lowBits) {
        String[] strings = toArrayOfShortStrings(highBits, lowBits);
        StringBuilder result = new StringBuilder();
        int[] shortHandNotationPositionAndLength =
                startAndLengthOfLongestRunOfZeroes(highBits, lowBits);
        int shortHandNotationPosition = shortHandNotationPositionAndLength[0];
        int shortHandNotationLength = shortHandNotationPositionAndLength[1];
        boolean useShortHandNotation = shortHandNotationLength > 1;

        for (int i = 0; i < strings.length; ++i) {
            if (useShortHandNotation && i == shortHandNotationPosition) {
                if (i == 0) {
                    result.append("::");
                } else {
                    result.append(":");
                }
            } else if (i <= shortHandNotationPosition
                    || i >= shortHandNotationPosition + shortHandNotationLength) {
                result.append(strings[i]);
                if (i < 7) {
                    result.append(":");
                }
            }
        }

        return result.toString().toLowerCase();
    }

    private static String[] toArrayOfShortStrings(long highBits, long lowBits) {
        short[] shorts = toShortArray(highBits, lowBits);
        String[] strings = new String[shorts.length];

        for (int i = 0; i < shorts.length; ++i) {
            strings[i] = String.format("%x", shorts[i]);
        }

        return strings;
    }

    private static short[] toShortArray(long highBits, long lowBits) {
        short[] shorts = new short[8];

        for (int i = 0; i < 8; ++i) {
            if (inHighRange(i)) {
                shorts[i] = (short) ((int) (highBits << i * 16 >>> 48 & 0xFFFF));
            } else {
                shorts[i] = (short) ((int) (lowBits << i * 16 >>> 48 & 0xFFFF));
            }
        }

        return shorts;
    }

    private static int[] startAndLengthOfLongestRunOfZeroes(long highBits, long lowBits) {
        int longestConsecutiveZeroes = 0;
        int longestConsecutiveZeroesPos = -1;
        short[] shorts = toShortArray(highBits, lowBits);

        for (int pos = 0; pos < shorts.length; ++pos) {
            int consecutiveZeroesAtCurrentPos = countConsecutiveZeroes(shorts, pos);
            if (consecutiveZeroesAtCurrentPos > longestConsecutiveZeroes) {
                longestConsecutiveZeroes = consecutiveZeroesAtCurrentPos;
                longestConsecutiveZeroesPos = pos;
            }
        }

        return new int[] {longestConsecutiveZeroesPos, longestConsecutiveZeroes};
    }

    private static boolean inHighRange(int shortNumber) {
        return shortNumber >= 0 && shortNumber < 4;
    }

    private static int countConsecutiveZeroes(short[] shorts, int offset) {
        int count = 0;

        for (int i = offset; i < shorts.length && shorts[i] == 0; ++i) {
            ++count;
        }

        return count;
    }

    public static String toIPv6String(long highBits, long lowBits) {

        if (isIPv4Mapped(highBits, lowBits)) {
            return toIPv4MappedAddressString(lowBits);
        } else if (isIPv4Compatibility(highBits, lowBits)) {
            return toIPv4CompatibilityAddressString(lowBits);
        }

        return toShortHandNotationString(highBits, lowBits);
    }

    public static String convertLongToIPv4Address(long lowBits) {
        return String.format(
                "%d.%d.%d.%d",
                (lowBits >> 24) & 0xff,
                (lowBits >> 16) & 0xff,
                (lowBits >> 8) & 0xff,
                lowBits & 0xff);
    }

    private static String toIPv4MappedAddressString(long lowBits) {
        return "::ffff:" + convertLongToIPv4Address(lowBits);
    }

    private static String toIPv4CompatibilityAddressString(long lowBits) {
        return "::" + convertLongToIPv4Address(lowBits);
    }

    /**
     * Returns true if the address is an IPv4-mapped IPv6 address. In these addresses, the first 80
     * bits are zero, the next 16 bits are one, and the remaining 32 bits are the IPv4 address.
     *
     * @return true if the address is an IPv4-mapped IPv6 addresses.
     */
    private static boolean isIPv4Mapped(long highBits, long lowBits) {
        return highBits == 0
                && (lowBits & 0xFFFF000000000000L) == 0
                && (lowBits & 0x0000FFFF00000000L) == 0x0000FFFF00000000L;
    }

    /**
     * Checks if the given IPv6 address is in IPv4 compatibility format. IPv4 compatibility format
     * is characterized by having the high 96 bits of the IPv6 address set to zero, while the low 32
     * bits represent an IPv4 address. The criteria for determining IPv4 compatibility format are as
     * follows: 1. The high 96 bits of the IPv6 address are all zeros. 2. The low 32 bits are within
     * the range from 0 to 4294967295 (0xFFFFFFFF). 3. The first 16 bits of the low 32 bits are all
     * ones (0xFFFF), indicating the special identifier for IPv4 compatibility format.
     *
     * @return True if the given IPv6 address is in IPv4 compatibility format; otherwise, false.
     */
    private static boolean isIPv4Compatibility(long highBits, long lowBits) {
        return highBits == 0L && lowBits <= 0xFFFFFFFFL && (lowBits & 65536L) == 65536L;
    }
}
