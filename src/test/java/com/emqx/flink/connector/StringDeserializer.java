package com.emqx.flink.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;

class StringDeserializer implements DeserializationSchema<String> {
    public String deserialize(byte[] raw) {
        return new String(raw);
    }

    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(new TypeHint<String>(){});
    }
}
