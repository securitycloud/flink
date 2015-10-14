/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 *
 * @author Lastovicka
 */
public class FlowSchema implements DeserializationSchema<Flow>, SerializationSchema<Flow, byte[]> {

    private static final long serialVersionUID = 1L;
    private final ObjectMapper mapper;

    public FlowSchema() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public Flow deserialize(byte[] message) {
        Flow flow;
        try {
            flow = mapper.readValue(message, Flow.class);
        } catch (IOException ex) {
            Logger.getLogger(Job.class.getName()).log(Level.SEVERE, null, ex);
            return new Flow();
        }
        return flow;
    }

    @Override
    public boolean isEndOfStream(Flow nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Flow element) {
        byte[] result = null;
        try {
            result = mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Job.class.getName()).log(Level.SEVERE, null, ex);
            //result = new byte[1];
        }
        return result;
    }

    @Override
    public TypeInformation<Flow> getProducedType() {
        return TypeExtractor.getForClass(Flow.class);
    }
}
