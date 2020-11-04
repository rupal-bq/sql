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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

    // Create directory to store transformed csv files
    CommandExecution.executeCommand("mkdir " + dataPath + "cassandra/");
    transformedDataPath = dataPath + "cassandra/";

    createQ1Data(dataPath + "lineitem.tbl", result);

    /*
    for (String tablename : CassandraTpchSchema.schemaMap.keySet()) {
      File table = new File(dataPath + tablename + ".tbl");
      if (!table.exists() || !table.isFile()) {
        throw new FileNotFoundException(tablename + ".tbl not found");
      }

      FileReader fileReader = new FileReader(table);
      BufferedReader reader = new BufferedReader(fileReader);
      int csvFilesNumber = 1;
      String filename = tablename + "_data_" + csvFilesNumber++ + ".csv";
      BufferedWriter writer = new BufferedWriter(
          new FileWriter(transformedDataPath + filename, true));
      int dataLineCounter = 0;
      try {
        result.addFile(transformedDataPath + filename);
        addSchemaToCSV(writer, tablename);
        String line;

        while ((line = reader.readLine()) != null) {
          List<String> argsList = Arrays.asList(line.split("\\|"));
          int fieldIndex = 0;
          for (String field : CassandraTpchSchema.schemaMap.get(tablename).keySet()) {
            String type = CassandraTpchSchema.schemaMap.get(tablename).get(field);

            if (type == "text" || type == "date") {
              writer.write("\'" + argsList.get(fieldIndex++) + "'");
            } else {
              writer.write(argsList.get(fieldIndex++));
            }

            if (fieldIndex < CassandraTpchSchema.schemaMap.get(tablename).size()) {
              writer.write(", ");
            }
          }

          writer.newLine();
          dataLineCounter++;

          if (dataLineCounter == 100 * (csvFilesNumber - 1)) {
            writer.close();
            filename = tablename + "_data_" + csvFilesNumber++ + ".csv";
            writer = new BufferedWriter(new FileWriter(transformedDataPath + filename, true));
            result.addFile(transformedDataPath + filename);
            addSchemaToCSV(writer, tablename);
          }
        }
      } finally {
        fileReader.close();
        writer.close();
      }
    }
    */

    return result;
  }

  private void addSchemaToCSV(BufferedWriter writer, String table) throws IOException {

    writer.write(CassandraTpchSchema.keyspaceName);
    writer.newLine();
    writer.write(table);
    writer.newLine();

    for (String field : CassandraTpchSchema.schemaMap.get(table).keySet()) {
      writer.write(" " + field + " " + CassandraTpchSchema.schemaMap.get(table).get(field) + ",");
    }
    int index = 1;
    writer.write(" PRIMARY KEY (");
    for (String key : CassandraTpchSchema.primaryKeyMap.get(table)) {
      writer.write(key);
      if (index < CassandraTpchSchema.primaryKeyMap.get(table).size()) {
        writer.write(", ");
        index++;
      }
    }
    writer.write(")");
    writer.newLine();

    index = 1;
    for (String field : CassandraTpchSchema.schemaMap.get(table).keySet()) {
      writer.write(field);
      if (index < CassandraTpchSchema.schemaMap.get(table).size()) {
        writer.write(", ");
        index++;
      }
    }
    writer.newLine();
  }

  private void createQ1Data(String lineitemPath, CassandraDataFormat fileList) throws Exception {
    File table = new File(lineitemPath);
    if (!table.exists() || !table.isFile()) {
      throw new FileNotFoundException("lineitem.tbl not found");
    }

    FileReader fileReader = new FileReader(table);
    BufferedReader reader = new BufferedReader(fileReader);

    int csvFilesNumber = 1;
    String filename = "TPCH_Q1_data_" + csvFilesNumber++ + ".csv";
    BufferedWriter writer = new BufferedWriter(
        new FileWriter(transformedDataPath + filename, true));

    try {
      fileList.addFile(transformedDataPath + filename);
      addQ1SchemaToCSV(writer);
      String line;

      int dataLineCounter = 0;
      while ((line = reader.readLine()) != null) {
        List<String> lineitemArgs = Arrays.asList(line.split("\\|"));

        String insertData = "'" + lineitemArgs.get(0) + "', "
            + "'" + lineitemArgs.get(9) + "', "
            + "'" + lineitemArgs.get(8) + "', "
            + lineitemArgs.get(4) + ", "
            + lineitemArgs.get(5) + ", "
            + lineitemArgs.get(6) + ", "
            + lineitemArgs.get(7) + ", "
            + "'" + lineitemArgs.get(10) + "'";

        writer.write(insertData);
        writer.newLine();
        dataLineCounter++;

        if (dataLineCounter == 100 * (csvFilesNumber - 1)) {
          writer.close();
          filename = "TPCH_Q1_data_" + csvFilesNumber++ + ".csv";
          writer = new BufferedWriter(new FileWriter(transformedDataPath + filename, true));
          fileList.addFile(transformedDataPath + filename);
          addQ1SchemaToCSV(writer);
        }
      }
    } finally {
      fileReader.close();
      writer.close();
    }
  }

  private void addQ1SchemaToCSV(BufferedWriter writer) throws IOException {
    writer.write(CassandraTpchSchema.keyspaceName);
    writer.newLine();
    writer.write("TPCH_Q1");
    writer.newLine();

    writer.write("orderkey text, linestatus text, returnflag text, quantity double, "
        + "extendedprice double, discount double, tax double, shipdate timestamp, "
        + "PRIMARY KEY ((returnflag,linestatus),shipdate,orderkey,linenumber)");
    writer.newLine();

    writer.write("orderkey, linestatus, returnflag, quantity, extendedprice, discount, tax, "
        + "shipdate");
    writer.newLine();
  }
}
