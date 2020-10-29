package com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load;

import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.cassandra.CassandraDataFormat;
import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.cassandra.CassandraDataTransformer;
import org.junit.jupiter.api.Test;

public class CassandraDataFormatTest {

  @Test
  public void testCassandraDataTransformer() throws Exception {
    CassandraDataTransformer dataTransformer = new CassandraDataTransformer();
    CassandraDataFormat dataFormat = (CassandraDataFormat) dataTransformer.transformData("/Users/rupalmahajan/Projects/GitHub/sql/benchmark/data/");
  }
}
