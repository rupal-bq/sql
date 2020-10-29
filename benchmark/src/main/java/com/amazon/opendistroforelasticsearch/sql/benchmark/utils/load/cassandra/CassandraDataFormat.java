/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.cassandra;

import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.DataFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Data format for Cassandra database.
 */
public class CassandraDataFormat extends DataFormat {

  private String keyspaceName = "benchmark_test_keyspace";

  // Map<tablename, Map<schema/insert,  createStatement/insertStatement>>
  private Map<String, Map<String, String>> tableSchemaAndInsertMap = new LinkedHashMap<>();

  /**
   * Returns tableSchemaAndInsertMap.
   *
   * @return Map of create & insert statements for all tables.
   */
  public Map<String, Map<String, String>> getTableSchemaAndInsertMap() {
    if (tableSchemaAndInsertMap.size() == 0) {
      createTableSchemaAndInsertMap();
    }
    return tableSchemaAndInsertMap;
  }

  private void createTableSchemaAndInsertMap() {
    for (String tablename : CassandraTpchSchema.schemaMap.keySet()) {
      String schema = "CREATE TABLE " + keyspaceName + "." + tablename + " (";
      String insert = "INSERT INTO " + keyspaceName + "." + tablename + " (";
      int i = 1;
      for (String field : CassandraTpchSchema.schemaMap.get(tablename).keySet()) {
        schema += " " + field + " " + CassandraTpchSchema.schemaMap.get(tablename).get(field) + " ";
        if (CassandraTpchSchema.primaryKeyMap.get(tablename).contains(field)) {
          schema += "PRIMARY KEY";
        }
        insert += " " + field + " ";
        if (i < CassandraTpchSchema.schemaMap.get(tablename).size()) {
          schema += ",";
          insert += ",";
          i++;
        }
      }
      schema += ")";
      insert += ") VALUES (";
      while (i > 1) {
        insert += "?, ";
        i--;
      }
      insert += "?)";

      Map<String, String> statementMap = new LinkedHashMap<>();
      statementMap.put("schema", schema);
      statementMap.put("insert", insert);
      tableSchemaAndInsertMap.put(tablename, statementMap);
    }
  }
}