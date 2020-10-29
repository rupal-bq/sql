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

import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.CommandExecution;
import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.DataFormat;
import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.DataTransformer;
import com.sun.jdi.InvalidTypeException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Data transformer for Cassandra database.
 */
public class CassandraDataTransformer implements DataTransformer {

  private String transformedDataPath;

  /**
   * Data transforming function for Cassandra.
   *
   * @param dataPath Directory for data to transform.
   * @return Path to transformed data.
   * @throws Exception Throws and exception if file read fails.
   */
  @Override
  public DataFormat transformData(String dataPath) throws Exception {
    File path = new File(dataPath);
    if (!path.exists() || !path.isDirectory()) {
      throw new FileNotFoundException("Invalid Directory");
    }

    CassandraDataFormat result = new CassandraDataFormat();
    Map<String, Map<String, String>> tableSchemaAndInsertMap = result.getTableSchemaAndInsertMap();

    // Create directory to store transformed SSTable files
    CommandExecution.executeCommand("mkdir " + dataPath + "cassandra/");
    transformedDataPath = dataPath + "cassandra/";

    for (String tableName : CassandraTpchSchema.schemaMap.keySet()) {
      File table = new File(dataPath + tableName + ".tbl");
      if (!table.exists() || !table.isFile()) {
        throw new FileNotFoundException(tableName + ".tbl not found");
      }

      FileReader fileReader = new FileReader(table);
      BufferedReader bufferedReader = new BufferedReader(fileReader);

      // Create new SSTable file for every  table / .tbl file
      CQLSSTableWriter writer = CQLSSTableWriter.builder()
          .inDirectory(transformedDataPath)
          .forTable(tableSchemaAndInsertMap.get(tableName).get("schema"))
          .using(tableSchemaAndInsertMap.get(tableName).get("insert"))
          .build();

      try {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          List<String> argsList = Arrays.asList(line.split("\\|"));
          int index=0;
          Map<String, Object> row = new HashMap<>();
          for(String field : CassandraTpchSchema.schemaMap.get(tableName).keySet()){
            row.put(field, getValue(tableName, field, argsList.get(index++)));
          }
          writer.addRow(row);
        }
      } finally {
        fileReader.close();
      }
    }

    return result;
  }

  private Object getValue(String table, String field, String value) throws Exception{
    String type = CassandraTpchSchema.schemaMap.get(table).get(field);
    if(type == "text"){
      return String.valueOf(value);
    }else if( type == "bigint"){
      return BigInteger.valueOf(Long.parseLong(value));
    }else if(type == "decimal"){
      return BigDecimal.valueOf(Long.parseLong(value));
    }else if(type == "int"){
      return Integer.valueOf(value);
    }else if(type == "date"){
      return Date.valueOf(value);
    }
    throw new InvalidTypeException("Invalid Type");
  }
}
