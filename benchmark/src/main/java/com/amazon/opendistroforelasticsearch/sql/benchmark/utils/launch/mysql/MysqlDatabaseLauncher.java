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

package com.amazon.opendistroforelasticsearch.sql.benchmark.utils.launch.mysql;

import static com.amazon.opendistroforelasticsearch.sql.benchmark.utils.CommandExecution.executeCommand;

import com.amazon.opendistroforelasticsearch.sql.benchmark.BenchmarkService;
import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.launch.DatabaseLauncher;

import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.mysql.MysqlTpchSchema;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Class to handle launching and shutting down MySQL databases.
 */
public class MysqlDatabaseLauncher implements DatabaseLauncher {

  private static final String URL = "jdbc:mysql://localhost/";

  /**
   * Function to launch an MySQL database.
   */
  @Override
  public void launchDatabase() throws IOException, InterruptedException {
    executeCommand(
        "echo " + BenchmarkService.systemPassword + " | sudo -S systemctl start mysql.service");
    executeCommand("echo " + BenchmarkService.systemPassword + " | sudo -S systemctl status mysql");
  }

  /**
   * Function to shutdown an MySQL database.
   */
  @Override
  public void close() throws Exception {
    String authUrl = URL + "?user=" + BenchmarkService.mysqlUsername + "&password="
        + BenchmarkService.mysqlPassword;
    Class.forName("com.mysql.cj.jdbc.Driver");
    Connection connection = DriverManager.getConnection(authUrl);
    Statement statement = connection.createStatement();
    statement.executeUpdate("drop database " + MysqlTpchSchema.databaseName);

    executeCommand(
        "echo " + BenchmarkService.systemPassword + " | sudo -S systemctl stop mysql.service");
    executeCommand("echo " + BenchmarkService.systemPassword + " | sudo -S systemctl status mysql");
  }
}
