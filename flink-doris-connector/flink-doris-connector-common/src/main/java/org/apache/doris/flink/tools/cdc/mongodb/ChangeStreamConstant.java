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

package org.apache.doris.flink.tools.cdc.mongodb;

import java.io.Serializable;

public class ChangeStreamConstant implements Serializable {
    private static final long serialVersionUID = 2599456667907755804L;
    public static final String ID_FIELD = "_id";
    public static final String OID_FIELD = "$oid";
    public static final String FIELD_TYPE = "operationType";
    public static final String FIELD_DATA = "fullDocument";
    public static final String OP_UPDATE = "update";
    public static final String OP_INSERT = "insert";
    public static final String OP_REPLACE = "replace";
    public static final String OP_DELETE = "delete";
    public static final String FIELD_DATABASE = "db";
    public static final String FIELD_TABLE = "coll";
    public static final String FIELD_NAMESPACE = "ns";
    public static final String FIELD_DOCUMENT_KEY = "documentKey";

    public static final String DATE_FIELD = "$date";
    public static final String TIMESTAMP_FIELD = "$timestamp";

    public static final String DECIMAL_FIELD = "$numberDecimal";

    public static final String LONG_FIELD = "$numberLong";
}
