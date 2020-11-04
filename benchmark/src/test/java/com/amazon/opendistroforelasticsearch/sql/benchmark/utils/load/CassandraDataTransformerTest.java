package com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load;

import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.load.cassandra.CassandraDataTransformer;
import org.junit.jupiter.api.Test;

public class CassandraDataTransformerTest {

  @Test
  void cassandraDataTransformerTest() throws Exception {
    CassandraDataTransformer transformer = new CassandraDataTransformer();
    DataFormat format = transformer
        .transformData("/Users/rupalmahajan/Projects/GitHub/sql/benchmark/data/");
  }
}
