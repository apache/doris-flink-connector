package org.apache.doris.flink.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface MockSourceFunction<T> extends SourceFunction<T> {}
