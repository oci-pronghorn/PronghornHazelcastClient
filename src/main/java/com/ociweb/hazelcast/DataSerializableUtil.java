package com.ociweb.hazelcast;

import com.hazelcast.nio.serialization.DataSerializable;

public class DataSerializableUtil {


    public <D extends DataSerializable> D decode(HazelcastClient client, HZDataInput reader) {
//        try {
//
//              DataSerializable ds = null;
//              if (reader.readBoolean()) {
//                  final DataSerializableFactory dsf = client.getFactory(reader.readInt());//TODO: cache last?
//                  ds = dsf.create(reader.readInt());//TODO: must catch null pointer
//              } else {
//                  String className = reader.readUTF();//string match on names of primitives then this class.
//                  ds = ClassLoaderUtil.newInstance(reader.getClass().getClassLoader(), className);
//              }
//
//         //     return ds.readData(reader);
//
//        } catch (IOException e) {
//           throw new RuntimeException(e);
//        } catch (NullPointerException npe) {
//            //diagnose was it the factory or create id which is missing?
//
//        }
//
        return null;
    }




}
