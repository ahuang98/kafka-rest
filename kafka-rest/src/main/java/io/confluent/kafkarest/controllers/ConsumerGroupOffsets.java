///*
// * Copyright 2020 Confluent Inc.
// *
// * Licensed under the Confluent Community License (the "License"); you may not use
// * this file except in compliance with the License.  You may obtain a copy of the
// * License at
// *
// * http://www.confluent.io/confluent-community-license
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package io.confluent.kafkarest.controllers;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Objects;
//import java.util.Set;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class ConsumerGroupOffsets {
//  public final Map<String, TopicOffsets> consumerGroupOffsets = new HashMap<>();
//
//  // ahu todo can we use this Logger in ConsumerGroupLagManagerImpl?
//  private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOffsets.class);
//  private final String consumerGroupId;
//  private String maxLagClientId;
//  private String maxLagConsumerId;
//  private String maxLagTopicName;
//  private int maxLagPartitionId;
//  private long sumCurrentOffset = 0;
//  private long sumEndOffset = 0;
//  private long maxLag = 0;
//  private long totalLag = 0;
//  private long totalLead = 0;
//  private Set<String> consumers = new HashSet<>();
//
//  // public ConsumerGroupOffsets(String consumerGroupId) {
//  //   this.consumerGroupId = consumerGroupId;
//  // }
//
//  public static class TopicOffsets {
//    public final String topic;
//    private long maxLag;
//    // private Set<Offset> topicOffsets = new HashSet<>();
//
//    public TopicOffsets(String topic) {
//      this.topic = topic;
//    }
//
//    public void addOffset(Offset offset) {
//      // if (topicOffsets.contains(offset)) {
//      //   log.warn("trying to add duplicated topic offsets data={}", offset);
//      //   return;
//      // }
//      // topicOffsets.add(offset);
//      if (maxLag < offset.getLag()) {
//        maxLag = offset.getLag();
//      }
//    }
//
//    // public Set<Offset> getTopicOffsets() {
//    //   return topicOffsets;
//    // }
//
//    public long getMaxLag() {
//      return maxLag;
//    }
//  }
//
//  public static class Offset {
//    public final String topic;
//    public final String consumerId;
//    public final String clientId;
//    public final int partition;
//    public final long currentOffset;
//    public final long beginningOffset;
//    public final long endOffset;
//
//    public Offset(
//        String topic,
//        String consumerId,
//        String clientId,
//        int partition,
//        long currentOffset,
//        long beginningOffset,
//        long endOffset
//    ) {
//      this.topic = topic;
//      this.consumerId = consumerId;
//      this.clientId = clientId;
//      this.partition = partition;
//      this.currentOffset = currentOffset;
//      this.beginningOffset = beginningOffset;
//      this.endOffset = endOffset;
//    }
//
//    public long getLag() {
//      return endOffset - currentOffset;
//    }
//
//    public long getLead() {
//      return currentOffset - beginningOffset;
//    }
//
//    public String getKey() {
//      return topic + "-" + consumerId + "-" + partition;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//      if (this == o) {
//        return true;
//      }
//      if (o == null || getClass() != o.getClass()) {
//        return false;
//      }
//      Offset that = (Offset) o;
//      return Objects.equals(topic, that.topic)
//          && Objects.equals(consumerId, that.consumerId)
//          && Objects.equals(clientId, that.clientId)
//          && Objects.equals(partition, that.partition);
//    }
//
//    @Override
//    public int hashCode() {
//      return Objects.hash(topic, consumerId, clientId, partition);
//    }
//  }
//
//  public String getConsumerGroupId() {
//    return consumerGroupId;
//  }
//
//  public String getMaxLagClientId() {
//    return maxLagClientId;
//  }
//
//  public String getMaxLagConsumerId() {
//    return maxLagConsumerId;
//  }
//
//  public String getMaxLagTopicName() {
//    return maxLagTopicName;
//  }
//
//  public int getMaxLagPartitionId() {
//    return maxLagPartitionId;
//  }
//
//  public long getMaxLag() {
//    return maxLag;
//  }
//
//  public long getTotalLag() {
//    return totalLag;
//  }
//
//  public long getTotalLead() {
//    return totalLead;
//  }
//
//  public long getSumCurrentOffset() {
//    return sumCurrentOffset;
//  }
//
//  public long getSumEndOffset() {
//    return sumEndOffset;
//  }
//
//   public int getNumTopics() {
//     return consumerGroupOffsets.size();
//   }
//
//  public int getNumConsumers() {
//    return consumers.size();
//  }
//
//  public void addOffset(
//      String topic,
//      String consumerId,
//      String clientId,
//      int partition,
//      long currentOffset,
//      long beginningOffset,
//      long endOffset
//  ) {
//    TopicOffsets topicOffsets = consumerGroupOffsets.get(topic);
//    if (topicOffsets == null) {
//      topicOffsets = new TopicOffsets(topic);
//      consumerGroupOffsets.put(topic, topicOffsets);
//    }
//
//    Offset offset = new Offset(
//        topic, consumerId, clientId, partition, currentOffset, beginningOffset, endOffset
//    );
//    topicOffsets.addOffset(offset);
//    if (maxLag < offset.getLag()) {
//      maxLag = offset.getLag();
//      maxLagClientId = clientId;
//      maxLagConsumerId = consumerId;
//      maxLagTopicName = topic;
//      maxLagPartitionId = partition;
//    }
//    totalLag += offset.getLag();
//    totalLead += offset.getLead();
//    this.sumCurrentOffset += offset.currentOffset;
//    this.sumEndOffset += offset.endOffset;
//
//    // MMA-3352: not adding consumers that are empty. this likely happens when a consumer group
//    //           has no active members. however we are calling addOffset to fix the issue of
//    //           lag data not showing up for groups w/o members like in the case of replicator
//    if (consumerId != null && !consumerId.isEmpty()) {
//      consumers.add(consumerId);
//    }
//  }
//}
