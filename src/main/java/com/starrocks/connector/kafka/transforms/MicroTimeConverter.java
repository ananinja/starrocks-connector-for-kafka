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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kafka Connect SMT that converts microsecond-since-midnight Long values to time strings
 * for specified fields in schemaless (Map) records.
 *
 * <p>PostgreSQL {@code time without time zone} columns are emitted by Debezium as
 * {@code io.debezium.time.MicroTime} — microseconds since midnight (INT64).
 * StarRocks cannot parse these integers as TIME or VARCHAR time values.
 * This SMT converts them to formatted time strings (e.g. 54000000000 → "15:00:00").
 *
 * <p>Intended to run <em>after</em> {@code AddOpFieldForDebeziumRecord} has flattened the
 * Debezium envelope into a plain Map record.
 *
 * <p>Example configuration:
 * <pre>
 *   transforms=addfield,convertTimes
 *   transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
 *   transforms.convertTimes.type=com.starrocks.connector.kafka.transforms.MicroTimeConverter
 *   transforms.convertTimes.fields=start_at,finish_at
 * </pre>
 */
public class MicroTimeConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(MicroTimeConverter.class);

    static final String FIELDS_CONFIG = "fields";
    static final String FORMAT_CONFIG = "format";
    private static final String DEFAULT_FORMAT = "HH:mm:ss";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Comma-separated list of field names holding microsecond-since-midnight Long values to convert to time strings.")
            .define(FORMAT_CONFIG,
                    ConfigDef.Type.STRING,
                    DEFAULT_FORMAT,
                    ConfigDef.Importance.LOW,
                    "Java DateTimeFormatter pattern for the output string (default: HH:mm:ss).");

    private Set<String> fields;
    private DateTimeFormatter formatter;

    @Override
    public void configure(Map<String, ?> configs) {
        String fieldsStr = (String) configs.get(FIELDS_CONFIG);
        this.fields = Arrays.stream(fieldsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        Object fmtObj = configs.get(FORMAT_CONFIG);
        String format = (fmtObj != null) ? fmtObj.toString() : DEFAULT_FORMAT;
        this.formatter = DateTimeFormatter.ofPattern(format);
    }

    @Override
    @SuppressWarnings("unchecked")
    public R apply(R record) {
        if (!(record.value() instanceof Map)) {
            return record;
        }

        Map<String, Object> original = (Map<String, Object>) record.value();
        Map<String, Object> updated = null;

        for (String field : fields) {
            Object raw = original.get(field);
            if (!(raw instanceof Number) || raw instanceof Double || raw instanceof Float) {
                // null, strings, or floating-point values are left as-is
                continue;
            }
            long micros = ((Number) raw).longValue();
            try {
                long totalSeconds = micros / 1_000_000L;
                String formatted = LocalTime.ofSecondOfDay(totalSeconds).format(formatter);
                if (updated == null) {
                    updated = new HashMap<>(original);
                }
                updated.put(field, formatted);
            } catch (Exception e) {
                LOG.warn("MicroTimeConverter: failed to convert field '{}' value={} — leaving as-is", field, micros, e);
            }
        }

        if (updated == null) {
            return record;
        }
        return record.newRecord(
                record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                null, updated,
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}
