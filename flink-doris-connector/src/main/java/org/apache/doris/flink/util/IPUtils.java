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

    public static void main(String[] args) {
        String s =
                fromBigInteger(new BigInteger("340282366920938463463374607431768211455"));
        System.out.println(s);
    }

    public static String fromBigInteger(BigInteger bigInteger) {
        byte[] bytes = bigInteger.toByteArray();
        long[] ipv6Bits =
                bytes[0] == 0
                        ? fromByteArray(
                                prefixWithZeroBytes(Arrays.copyOfRange(bytes, 1, bytes.length)))
                        : fromByteArray(prefixWithZeroBytes(bytes));
        return toIPv6String(ipv6Bits[0], ipv6Bits[1]);
    }

    private static byte[] prefixWithZeroBytes(byte[] original) {
        byte[] target = new byte[16];
        System.arraycopy(original, 0, target, 16 - original.length, original.length);
        return target;
    }

    public static long[] fromByteArray(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("can not construct from [null]");
        } else if (bytes.length != 16) {
            throw new IllegalArgumentException(
                    "the byte array to construct from should be 16 bytes long");
        } else {
            ByteBuffer buf = ByteBuffer.allocate(16);

            for (byte b : bytes) {
                buf.put(b);
            }

            buf.rewind();
            LongBuffer longBuffer = buf.asLongBuffer();
            return new long[] {longBuffer.get(), longBuffer.get()};
        }
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
                shorts[i] = (short) ((int) (highBits << i * 16 >>> 48 & 65535L));
            } else {
                shorts[i] = (short) ((int) (lowBits << i * 16 >>> 48 & 65535L));
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
        return isIPv4Mapped(highBits, lowBits)
                ? toIPv4MappedAddressString(lowBits)
                : toShortHandNotationString(highBits, lowBits);
    }

    private static String toIPv4MappedAddressString(long lowBits) {
        int byteZero = (int) ((lowBits & 4278190080L) >> 24);
        int byteOne = (int) ((lowBits & 16711680L) >> 16);
        int byteTwo = (int) ((lowBits & 65280L) >> 8);
        int byteThree = (int) (lowBits & 255L);
        return "::ffff:" + byteZero + "." + byteOne + "." + byteTwo + "." + byteThree;
    }

    private static boolean isIPv4Mapped(long highBits, long lowBits) {
        return highBits == 0L
                && (lowBits & -281474976710656L) == 0L
                && (lowBits & 281470681743360L) == 281470681743360L;
    }

    public static byte[] prefixWithZeroBytes(byte[] original, int newSize) {
        byte[] target = new byte[newSize];
        System.arraycopy(original, 0, target, newSize - original.length, original.length);
        return target;
    }
}
