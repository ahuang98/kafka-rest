package io.confluent.kafkarest;

import static java.util.Collections.emptyList;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import io.confluent.kafkarest.entities.Partition;

public class PartitionDeserializer extends KeyDeserializer {

  @Override
  public Partition deserializeKey(
      String key,
      DeserializationContext ctxt
  ) {
    return Partition.create(
        "clusterId_here",
        "topicName_here",
        42,
        emptyList());
  }

}
