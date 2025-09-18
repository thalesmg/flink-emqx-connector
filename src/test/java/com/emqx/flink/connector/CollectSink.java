package com.emqx.flink.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectSink<OUT> implements Sink<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(CollectSink.class);

    static private AtomicInteger count = new AtomicInteger();
    private ArrayList<OUT> results = new ArrayList<>();

    public int getCount() {
        return count.get();
    }

    @Override
    public SinkWriter<OUT> createWriter(WriterInitContext arg0) throws IOException {
        count.set(0);
        return new CollectSinkWriter();
    }

    public class CollectSinkWriter implements SinkWriter<OUT> {
        @Override
        public void close() throws Exception {
        }

        @Override
        public void flush(boolean arg0) throws IOException, InterruptedException {
        }

        @Override
        public void write(OUT element, Context context) throws IOException, InterruptedException {
            LOG.info("received element: {}", element);
            results.add(element);
            int currentCount = count.incrementAndGet();
            LOG.info("current count: {}", currentCount);
        }
    }
}
