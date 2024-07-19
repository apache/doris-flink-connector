// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.util;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class GeoUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static JsonNode convertMysqlGeometryToJson(Struct geometryStruct) {
        try {
            byte[] wkb = geometryStruct.getBytes("wkb");
            String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
            JsonNode originGeoNode = OBJECT_MAPPER.readTree(geoJson);
            Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
            Map<String, Object> geometryInfo = new HashMap<>();
            String geometryType = originGeoNode.get("type").asText();
            geometryInfo.put("type", geometryType);
            if (geometryType.equals("GeometryCollection")) {
                geometryInfo.put("geometries", originGeoNode.get("geometries"));
            } else {
                geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
            }
            geometryInfo.put("srid", srid.orElse(0));
            return OBJECT_MAPPER.valueToTree(geometryInfo);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Failed to convert %s to geometry JSON.", geometryStruct), e);
        }
    }
}
