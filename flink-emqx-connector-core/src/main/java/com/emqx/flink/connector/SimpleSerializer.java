package com.emqx.flink.connector;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

// TODO: use another serializer implementation for real; just for PoC
public class SimpleSerializer<T> implements SimpleVersionedSerializer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSerializer.class);

    @Override
    public byte[] serialize(T obj) throws IOException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(boas);
        oos.writeObject(obj);
        return boas.toByteArray();
    }

    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            T res = (T) ois.readObject();
            return res;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
