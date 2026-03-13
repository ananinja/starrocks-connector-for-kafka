/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kafka.transforms;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MicroTimeConverterTest {

    private SinkRecord recordWithMap(Map<String, Object> value) {
        return new SinkRecord("topic", 0, null, null, null, value, 0);
    }

    private MicroTimeConverter<SinkRecord> configure(String fields) {
        MicroTimeConverter<SinkRecord> smt = new MicroTimeConverter<>();
        Map<String, String> props = new HashMap<>();
        props.put(MicroTimeConverter.FIELDS_CONFIG, fields);
        smt.configure(props);
        return smt;
    }

    @Test
    public void convertsStartAt() {
        // 54000000000 µs = 54000 s = 15:00:00
        Map<String, Object> value = new HashMap<>();
        value.put("start_at", 54000000000L);
        value.put("status", 1);

        MicroTimeConverter<SinkRecord> smt = configure("start_at");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("15:00:00", resultValue.get("start_at"));
        Assert.assertEquals(1, resultValue.get("status"));
    }

    @Test
    public void convertsFinishAt() {
        // 73800000000 µs = 73800 s = 20:30:00
        Map<String, Object> value = new HashMap<>();
        value.put("finish_at", 73800000000L);

        MicroTimeConverter<SinkRecord> smt = configure("finish_at");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("20:30:00", resultValue.get("finish_at"));
    }

    @Test
    public void convertsMultipleFields() {
        Map<String, Object> value = new HashMap<>();
        value.put("start_at", 25200000000L);   // 07:00:00
        value.put("finish_at", 68400000000L);  // 19:00:00

        MicroTimeConverter<SinkRecord> smt = configure("start_at,finish_at");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("07:00:00", resultValue.get("start_at"));
        Assert.assertEquals("19:00:00", resultValue.get("finish_at"));
    }

    @Test
    public void midnightIsZero() {
        // 0 µs = 00:00:00
        Map<String, Object> value = new HashMap<>();
        value.put("start_at", 0L);

        MicroTimeConverter<SinkRecord> smt = configure("start_at");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("00:00:00", resultValue.get("start_at"));
    }

    @Test
    public void nullFieldPassesThrough() {
        Map<String, Object> value = new HashMap<>();
        value.put("start_at", (Object) null);

        MicroTimeConverter<SinkRecord> smt = configure("start_at");
        SinkRecord record = recordWithMap(value);
        SinkRecord result = smt.apply(record);
        // null → no update → same record instance returned
        Assert.assertSame(record, result);
    }

    @Test
    public void stringFieldPassesThrough() {
        // Already-converted fields must not be double-converted
        Map<String, Object> value = new HashMap<>();
        value.put("start_at", "15:00:00");

        MicroTimeConverter<SinkRecord> smt = configure("start_at");
        SinkRecord record = recordWithMap(value);
        SinkRecord result = smt.apply(record);

        Assert.assertSame(record, result);
    }

    @Test
    public void nonMapRecordPassesThrough() {
        MicroTimeConverter<SinkRecord> smt = configure("start_at");
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, "plain-string", 0);
        SinkRecord result = smt.apply(record);
        Assert.assertSame(record, result);
    }

    @Test
    public void returnsOriginalWhenNoFieldsMatch() {
        Map<String, Object> value = new HashMap<>();
        value.put("status", 1);

        MicroTimeConverter<SinkRecord> smt = configure("start_at");
        SinkRecord record = recordWithMap(value);
        SinkRecord result = smt.apply(record);
        Assert.assertSame(record, result);
    }

    @Test
    public void integerValueAlsoConverted() {
        // io.debezium.time.MicroTime may arrive as Integer in some edge cases
        Map<String, Object> value = new HashMap<>();
        value.put("start_at", 3600000000);  // int: 01:00:00

        MicroTimeConverter<SinkRecord> smt = configure("start_at");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("01:00:00", resultValue.get("start_at"));
    }
}
