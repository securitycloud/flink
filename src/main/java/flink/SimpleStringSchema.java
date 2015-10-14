/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 *
 * @author Lastovicka
 */
public class SimpleStringSchema implements DeserializationSchema<String>, SerializationSchema<String, byte[]> {
		private static final long serialVersionUID = 1L;

		public SimpleStringSchema() {
		}

                @Override
		public String deserialize(byte[] message) {
			return new String(message);
		}

                @Override
		public boolean isEndOfStream(String nextElement) {
			return false;
		}

                @Override
		public byte[] serialize(String element) {
                        if(element == null){
                            return null;
                        }
			return element.getBytes();
		}

                @Override
		public TypeInformation<String> getProducedType() {
			return TypeExtractor.getForClass(String.class);
		}
	}
