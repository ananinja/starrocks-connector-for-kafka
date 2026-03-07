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

public class EpochDayConverterTest {

    private SinkRecord recordWithMap(Map<String, Object> value) {
        return new SinkRecord("topic", 0, null, null, null, value, 0);
    }

    private EpochDayConverter<SinkRecord> configure(String fields) {
        EpochDayConverter<SinkRecord> smt = new EpochDayConverter<>();
        Map<String, String> props = new HashMap<>();
        props.put(EpochDayConverter.FIELDS_CONFIG, fields);
        smt.configure(props);
        return smt;
    }

    @Test
    public void convertsKnownDateField() {
        // 20518 days since 1970-01-01 = 2026-03-07
        Map<String, Object> value = new HashMap<>();
        value.put("postingDate", 20518);
        value.put("itemNo", "103827");

        EpochDayConverter<SinkRecord> smt = configure("postingDate");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("2026-03-07", resultValue.get("postingDate"));
        Assert.assertEquals("103827", resultValue.get("itemNo"));
    }

    @Test
    public void convertsMultipleFields() {
        Map<String, Object> value = new HashMap<>();
        value.put("postingDate", 20518);    // 2026-03-07
        value.put("documentDate", 20518);   // 2026-03-07
        value.put("expirationDate", (Object) null); // nullable — must stay null

        EpochDayConverter<SinkRecord> smt = configure("postingDate,documentDate,expirationDate");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("2026-03-07", resultValue.get("postingDate"));
        Assert.assertEquals("2026-03-07", resultValue.get("documentDate"));
        Assert.assertNull(resultValue.get("expirationDate"));
    }

    @Test
    public void convertsLongValue() {
        // epoch day can also arrive as Long
        Map<String, Object> value = new HashMap<>();
        value.put("postingDate", 20518L);

        EpochDayConverter<SinkRecord> smt = configure("postingDate");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("2026-03-07", resultValue.get("postingDate"));
    }

    @Test
    public void doesNotModifyStringFields() {
        // Already-converted fields must pass through unchanged
        Map<String, Object> value = new HashMap<>();
        value.put("postingDate", "2026-03-07");

        EpochDayConverter<SinkRecord> smt = configure("postingDate");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("2026-03-07", resultValue.get("postingDate"));
    }

    @Test
    public void epochZeroIsUnixEpoch() {
        Map<String, Object> value = new HashMap<>();
        value.put("postingDate", 0);

        EpochDayConverter<SinkRecord> smt = configure("postingDate");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("1970-01-01", resultValue.get("postingDate"));
    }

    @Test
    public void passesThroughNonMapRecord() {
        EpochDayConverter<SinkRecord> smt = configure("postingDate");
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, "plain-string", 0);
        SinkRecord result = smt.apply(record);
        Assert.assertSame(record, result);
    }

    @Test
    public void returnsOriginalRecordWhenNoFieldsMatch() {
        Map<String, Object> value = new HashMap<>();
        value.put("itemNo", "103827");

        EpochDayConverter<SinkRecord> smt = configure("postingDate");
        SinkRecord record = recordWithMap(value);
        SinkRecord result = smt.apply(record);
        Assert.assertSame(record, result);
    }

    @Test
    public void customFormat() {
        Map<String, Object> value = new HashMap<>();
        value.put("postingDate", 20518);

        EpochDayConverter<SinkRecord> smt = new EpochDayConverter<>();
        Map<String, String> props = new HashMap<>();
        props.put(EpochDayConverter.FIELDS_CONFIG, "postingDate");
        props.put(EpochDayConverter.FORMAT_CONFIG, "dd/MM/yyyy");
        smt.configure(props);

        SinkRecord result = smt.apply(recordWithMap(value));
        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("07/03/2026", resultValue.get("postingDate"));
    }
}
