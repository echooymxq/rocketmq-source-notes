/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

public class DataConverter {
    public static Charset charset = Charset.forName("UTF-8");

    public static byte[] Long2Byte(Long v) {
        ByteBuffer tmp = ByteBuffer.allocate(8);
        tmp.putLong(v);
        return tmp.array();
    }

    public static int setBit(int value, int index, boolean flag) {
        if (flag) {
            return (int) (value | (1L << index));
        } else {
            return (int) (value & ~(1L << index));
        }
    }

    public static boolean getBit(int value, int index) {
        // 将值左移index位和Value按位与
        return (value & (1L << index)) != 0;
    }

    private static void markBitCAS(AtomicInteger setBits, int index) {
        while (true) {
            int bits = setBits.get();
            if (DataConverter.getBit(bits, index)) {
                break;
            }

            int newBits = DataConverter.setBit(bits, index, true);
            if (setBits.compareAndSet(bits, newBits)) {
                break;
            }
        }
    }

    public static void main(String[] args) {
        AtomicInteger bits = new AtomicInteger();
        AtomicInteger storeBits = new AtomicInteger();
        markBitCAS(bits, 0);
        markBitCAS(bits, 1);
//        markBitCAS(storeBits, 1);

        int num = 5;
        boolean finish = true;
        // 1 1
        int i = bits.get();
        // 1 1
        int i1 = storeBits.get();
        int b = i ^ i1;
        for (byte j = 0; j < num; j++) {
            if (DataConverter.getBit(b, j)) {
                finish = false;
            }
        }

        System.out.println();
        //
    }

}
