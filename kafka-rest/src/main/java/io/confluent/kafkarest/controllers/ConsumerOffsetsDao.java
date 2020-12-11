/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.controllers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.confluent.kafkarest.config.ConfigModule.OffsetsTimeoutMsConfig;
//import io.confluent.kafkarest.controllers.ConsumerGroupOffsets.Offset;
//import io.confluent.kafkarest.controllers.ConsumerGroupOffsets.TopicOffsets;
import io.confluent.kafkarest.entities.v3.ConsumerGroupLagData;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsumerOffsetsDao {

  private final Admin kafkaAdminClient;
  private final int consumerMetadataTimeout;

  @Inject
  ConsumerOffsetsDao(
      Admin kafkaAdminClient,
      @OffsetsTimeoutMsConfig int consumerMetadataTimeout
  ) {
    this.kafkaAdminClient = kafkaAdminClient;
    this.consumerMetadataTimeout = consumerMetadataTimeout;
  }

  //  public Map<String, ConsumerGroupOffsets> getAllConsumerGroupOffsets()
  //      throws InterruptedException, ExecutionException, TimeoutException {
  //
  //    final Set<String> consumerGroupIds = getConsumerGroups();
  //    final Map<String, ConsumerGroupDescription> fetchedConsumerGroupDesc =
  //        getAllConsumerGroupDescriptions(consumerGroupIds);
  //    final Map<String, ConsumerGroupOffsets> cgOffsetsMap = new HashMap<>();
  //
  //    // all consumer groups for this cluster
  //    for (ConsumerGroupDescription desc : fetchedConsumerGroupDesc.values()) {
  //      ConsumerGroupOffsets cgOffsets = getConsumerGroupOffsets(
  //      desc, IsolationLevel.READ_COMMITTED);
  //      cgOffsetsMap.put(desc.groupId(), cgOffsets);
  //    }
  //
  //    return cgOffsetsMap;
  //  }

  // ConsumerGroupLagData.Builder getConsumerGroupOffsets(
  CompletableFuture<ConsumerGroupLagData.Builder> getConsumerGroupOffsets(
      ConsumerGroupDescription cgDesc,
      Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets,
      Map<TopicPartition, ListOffsetsResultInfo> earliestOffsets,
      Map<TopicPartition, ListOffsetsResultInfo> latestOffsets
  ) {

    // build map of topic partition -> consumer id
    Map<TopicPartition, String> tpConsumerIds = new HashMap<>();
    Map<TopicPartition, String> tpClientIds = new HashMap<>();

    for (MemberDescription memberDesc : cgDesc.members()) {
      for (TopicPartition tp : memberDesc.assignment().topicPartitions()) {
        tpConsumerIds.put(tp, memberDesc.consumerId());
        tpClientIds.put(tp, memberDesc.clientId());
      }
    }

    for (TopicPartition tp : fetchedCurrentOffsets.keySet()) {
      // look up consumer id from map if not part of a simple consumer group
      String consumerId = tpConsumerIds.getOrDefault(tp, "");
      String clientId = tpClientIds.getOrDefault(tp, "");

      long currentOffset = getCurrentOffset(fetchedCurrentOffsets, tp);
      long latestOffset = getOffset(latestOffsets, tp);
      long earliestOffset = getOffset(earliestOffsets, tp);
      if (currentOffset < 0 || latestOffset < 0) {
        // log.debug("invalid offsets for topic={} consumerId={} current={} latest={}",
        //     tp.topic(),
        //     consumerId,
        //     currentOffset,
        //     latestOffset);
        continue;
      }
      addOffset(
          tp.topic(),
          consumerId,
          clientId,
          tp.partition(),
          currentOffset,
          earliestOffset,
          latestOffset
      );
    }
    return CompletableFuture.completedFuture(ConsumerGroupLagData.builder()
        .setClusterId("pass in from ConsumerGroupLagManagerImpl, or don't set and return builder")
        .setConsumerGroupId("pass in as parameter, or ...")
        .setMaxLag(maxLag)
        .setTotalLag(totalLag)
        .setMaxLagConsumerId(maxLagConsumerId)
        .setMaxLagClientId(maxLagClientId)
        .setMaxLagInstanceId("todo")
        .setMaxLagPartitionId(maxLagPartitionId));
  }

  public CompletableFuture<ConsumerGroupLagData.Builder> getConsumerGroupOffsets(
      String consumerGroupId,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    final ConsumerGroupDescription cgDesc = getConsumerGroupDescription(consumerGroupId);
    return getConsumerGroupOffsets(cgDesc, isolationLevel);
  }

  private CompletableFuture<ConsumerGroupLagData.Builder> getConsumerGroupOffsets(
      ConsumerGroupDescription desc,
      IsolationLevel isolationLevel
  ) throws InterruptedException, ExecutionException, TimeoutException {
    final Map<TopicPartition, OffsetAndMetadata> fetchedCurrentOffsets =
        getCurrentOffsets(desc.groupId());

    ListOffsetsOptions listOffsetsOptions = new ListOffsetsOptions(isolationLevel)
        .timeoutMs(consumerMetadataTimeout);

    Map<TopicPartition, OffsetSpec> latestOffsetSpecs = fetchedCurrentOffsets.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
    Map<TopicPartition, OffsetSpec> earliestOffsetSpecs = fetchedCurrentOffsets.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.earliest()));

    ListOffsetsResult latestOffsetResult = kafkaAdminClient.listOffsets(
        latestOffsetSpecs, listOffsetsOptions);
    ListOffsetsResult earliestOffsetResult = kafkaAdminClient.listOffsets(
        earliestOffsetSpecs, listOffsetsOptions);
    Map<TopicPartition, ListOffsetsResultInfo> latestOffsets = latestOffsetResult.all()
        .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);
    Map<TopicPartition, ListOffsetsResultInfo> earliestOffsets = earliestOffsetResult.all()
        .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);

    return getConsumerGroupOffsets(
        desc,
        fetchedCurrentOffsets,
        earliestOffsets,
        latestOffsets
    );
  }

  public Set<String> getConsumerGroups()
      throws InterruptedException, ExecutionException, TimeoutException {
    return Sets.newLinkedHashSet(
        Iterables.transform(kafkaAdminClient
                .listConsumerGroups(new ListConsumerGroupsOptions()
                    .timeoutMs(consumerMetadataTimeout))
                .all()
                .get(consumerMetadataTimeout, TimeUnit.MILLISECONDS),
            ConsumerGroupListing::groupId));
  }

  public ConsumerGroupDescription getConsumerGroupDescription(
      String consumerGroupId
  ) throws InterruptedException, ExecutionException, TimeoutException {
    Map<String, ConsumerGroupDescription> allCgDesc =
        getAllConsumerGroupDescriptions(ImmutableSet.of(consumerGroupId));
    return allCgDesc.get(consumerGroupId);
  }

  public Map<String, ConsumerGroupDescription> getAllConsumerGroupDescriptions(
      Collection<String> consumerGroupIds
  ) throws InterruptedException, ExecutionException, TimeoutException {

    final Map<String, ConsumerGroupDescription> ret = new HashMap<>();

    KafkaFuture.allOf(Iterables.toArray(
        Iterables.transform(
            kafkaAdminClient
                .describeConsumerGroups(
                    consumerGroupIds,
                    new DescribeConsumerGroupsOptions()
                        .includeAuthorizedOperations(true)
                        .timeoutMs(consumerMetadataTimeout)
                )
                .describedGroups().entrySet(),
            entry -> {
              final String cgId = entry.getKey();
              return entry.getValue().whenComplete(
                  (cgDesc, throwable) -> {
                    if (throwable != null) {
                      // log.warn("failed fetching description for consumerGroup={}", cgId,
                      //     throwable);
                    } else if (cgDesc != null) {
                      ret.put(cgId, cgDesc);
                    }
                  }
              );
            }
        ), KafkaFuture.class)
    ).get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);

    return ret;
  }

  public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets(
      String consumerGroupId
  ) throws InterruptedException, ExecutionException, TimeoutException {
    return kafkaAdminClient
        .listConsumerGroupOffsets(consumerGroupId,
            new ListConsumerGroupOffsetsOptions().timeoutMs(consumerMetadataTimeout))
        .partitionsToOffsetAndMetadata().get(consumerMetadataTimeout, TimeUnit.MILLISECONDS);
  }

  private long getCurrentOffset(Map<TopicPartition, OffsetAndMetadata> map, TopicPartition tp) {
    if (map == null) {
      return -1;
    }
    OffsetAndMetadata oam = map.get(tp);
    if (oam == null) {
      return -1;
    }
    return oam.offset();
  }

  private long getOffset(Map<TopicPartition, ListOffsetsResultInfo> map, TopicPartition tp) {
    if (map == null) {
      return -1;
    }

    ListOffsetsResultInfo offsetInfo = map.get(tp);
    if (offsetInfo == null) {
      return -1;
    }

    return offsetInfo.offset();
  }

  // @Override
  // public void close() {
  //   kafkaAdminClient.close();
  // }

  private final Map<String, TopicOffsets> consumerGroupOffsets = new HashMap<>();

  // ahu todo can we use this Logger in ConsumerGroupLagManagerImpl?
  // private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOffsets.class);
  private String maxLagClientId;
  private String maxLagConsumerId;
  private String maxLagTopicName;
  private int maxLagPartitionId;
  private long sumCurrentOffset = 0;
  private long sumEndOffset = 0;
  private long maxLag = 0;
  private long totalLag = 0;
  private long totalLead = 0;
  private Set<String> consumers = new HashSet<>();

  private class TopicOffsets {
    final String topic;
    long maxLag;
    // private Set<Offset> topicOffsets = new HashSet<>();

    private TopicOffsets(String topic) {
      this.topic = topic;
    }

    private void addOffset(Offset offset) {
      // if (topicOffsets.contains(offset)) {
      //   log.warn("trying to add duplicated topic offsets data={}", offset);
      //   return;
      // }
      // topicOffsets.add(offset);
      if (maxLag < offset.getLag()) {
        maxLag = offset.getLag();
      }
    }

    // public Set<Offset> getTopicOffsets() {
    //   return topicOffsets;
    // }

  }

  private class Offset {
    final String topic;
    final String consumerId;
    final String clientId;
    final int partition;
    final long currentOffset;
    final long beginningOffset;
    final long endOffset;

    private Offset(
        String topic,
        String consumerId,
        String clientId,
        int partition,
        long currentOffset,
        long beginningOffset,
        long endOffset
    ) {
      this.topic = topic;
      this.consumerId = consumerId;
      this.clientId = clientId;
      this.partition = partition;
      this.currentOffset = currentOffset;
      this.beginningOffset = beginningOffset;
      this.endOffset = endOffset;
    }

    public long getLag() {
      return endOffset - currentOffset;
    }

    public long getLead() {
      return currentOffset - beginningOffset;
    }

    public String getKey() {
      return topic + "-" + consumerId + "-" + partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Offset that = (Offset) o;
      return Objects.equals(topic, that.topic)
          && Objects.equals(consumerId, that.consumerId)
          && Objects.equals(clientId, that.clientId)
          && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, consumerId, clientId, partition);
    }
  }
  public void addOffset(
      String topic,
      String consumerId,
      String clientId,
      int partition,
      long currentOffset,
      long beginningOffset,
      long endOffset
  ) {
    TopicOffsets topicOffsets = consumerGroupOffsets.get(topic);
    if (topicOffsets == null) {
      topicOffsets = new TopicOffsets(topic);
      consumerGroupOffsets.put(topic, topicOffsets);
    }

    Offset offset = new Offset(
        topic, consumerId, clientId, partition, currentOffset, beginningOffset, endOffset
    );
    topicOffsets.addOffset(offset);
    if (maxLag < offset.getLag()) {
      maxLag = offset.getLag();
      maxLagClientId = clientId;
      maxLagConsumerId = consumerId;
      maxLagTopicName = topic;
      maxLagPartitionId = partition;
    }
    totalLag += offset.getLag();
    totalLead += offset.getLead();
    this.sumCurrentOffset += offset.currentOffset;
    this.sumEndOffset += offset.endOffset;

    // MMA-3352: not adding consumers that are empty. this likely happens when a consumer group
    //           has no active members. however we are calling addOffset to fix the issue of
    //           lag data not showing up for groups w/o members like in the case of replicator
    if (consumerId != null && !consumerId.isEmpty()) {
      consumers.add(consumerId);
    }
  }
}
