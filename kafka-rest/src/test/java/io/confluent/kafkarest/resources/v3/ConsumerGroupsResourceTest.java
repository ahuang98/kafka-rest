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

package io.confluent.kafkarest.resources.v3;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.controllers.ConsumerGroupManager;
import io.confluent.kafkarest.entities.Broker;
import io.confluent.kafkarest.entities.Consumer;
import io.confluent.kafkarest.entities.ConsumerGroup;
import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.v3.ConsumerGroupData;
import io.confluent.kafkarest.entities.v3.ConsumerGroupDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerGroupsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.response.CrnFactoryImpl;
import io.confluent.kafkarest.response.FakeAsyncResponse;
import io.confluent.kafkarest.response.FakeUrlFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.NotFoundException;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerGroupsResourceTest {

  private static final String CLUSTER_ID = "cluster-1";

  private static final Broker BROKER_1 =
      Broker.create(
          CLUSTER_ID,
          /* brokerId= */ 1,
          /* host= */ "1.2.3.4",
          /* port= */ 1000,
          /* rack= */ null);
  private static final Broker BROKER_2 =
      Broker.create(
          CLUSTER_ID,
          /* brokerId= */ 2,
          /* host= */ "5.6.7.8",
          /* port= */ 2000,
          /* rack= */ null);

  private static final Partition[][] CONSUMER_LIST_1_PARTITIONS =
      {
          {
              createPartition("topic-1", 1),
              createPartition("topic-2", 2),
              createPartition("topic-3", 3)
          },
          {
              createPartition("topic-1", 4),
              createPartition("topic-2", 5),
              createPartition("topic-3", 6)
          },
          {
              createPartition("topic-1", 7),
              createPartition("topic-2", 8),
              createPartition("topic-3", 9)
          }
      };

  private static final Partition[][] CONSUMER_LIST_2_PARTITIONS =
      {
          {
              createPartition("topic-4", 1),
              createPartition("topic-5", 2),
              createPartition("topic-6", 3)
          },
          {
              createPartition("topic-4", 4),
              createPartition("topic-5", 5),
              createPartition("topic-6", 6)
          },
          {
              createPartition("topic-7", 7),
              createPartition("topic-8", 8),
              createPartition("topic-9", 9)
          }
      };

  private static final Consumer[] CONSUMER_LIST_1 =
      {
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-1")
              .setConsumerId("consumer-1")
              .setClientId("client-1")
              .setInstanceId("instance-1")
              .setHost("11.12.12.14")
              .setAssignedPartitions(Arrays.asList(CONSUMER_LIST_1_PARTITIONS[0]))
              .build(),
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-1")
              .setConsumerId("consumer-2")
              .setClientId("client-2")
              .setInstanceId("instance-2")
              .setHost("21.22.23.24")
              .setAssignedPartitions(Arrays.asList(CONSUMER_LIST_1_PARTITIONS[1]))
              .build(),
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-1")
              .setConsumerId("consumer-3")
              .setClientId("client-3")
              .setInstanceId("instance-3")
              .setHost("31.32.33.34")
              .setAssignedPartitions(Arrays.asList(CONSUMER_LIST_1_PARTITIONS[2]))
              .build()
      };

  private static final Consumer[] CONSUMER_LIST_2 =
      {
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-2")
              .setConsumerId("consumer-4")
              .setClientId("client-4")
              .setInstanceId("instance-4")
              .setHost("41.42.43.44")
              .setAssignedPartitions(Arrays.asList(CONSUMER_LIST_2_PARTITIONS[0]))
              .build(),
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-2")
              .setConsumerId("consumer-5")
              .setClientId("client-5")
              .setInstanceId("instance-5")
              .setHost("51.52.53.54")
              .setAssignedPartitions(Arrays.asList(CONSUMER_LIST_2_PARTITIONS[1]))
              .build(),
          Consumer.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-2")
              .setConsumerId("consumer-6")
              .setClientId("client-6")
              .setInstanceId("instance-6")
              .setHost("61.62.63.64")
              .setAssignedPartitions(Arrays.asList(CONSUMER_LIST_2_PARTITIONS[2]))
              .build()
      };

  private static Map<Partition, Consumer> PARTITION_ASSIGNMENT_1;
  static {
    PARTITION_ASSIGNMENT_1 = new HashMap<>();
    for (int c = 0; c < CONSUMER_LIST_1.length; c++) {
      for (Partition partition : CONSUMER_LIST_1_PARTITIONS[c]) {
        PARTITION_ASSIGNMENT_1.put(partition, CONSUMER_LIST_1[c]);
      }
    }
  }

  private static Map<Partition, Consumer> PARTITION_ASSIGNMENT_2;
  static {
    PARTITION_ASSIGNMENT_2 = new HashMap<>();
    for (int c = 0; c < CONSUMER_LIST_2.length; c++) {
      for (Partition partition : CONSUMER_LIST_2_PARTITIONS[c]) {
        PARTITION_ASSIGNMENT_2.put(partition, CONSUMER_LIST_2[c]);
      }
    }
  }

  private static final ConsumerGroup[] CONSUMER_GROUPS =
      {
          ConsumerGroup.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-1")
              .setSimple(true)
              .setPartitionAssignor("org.apache.kafka.clients.consumer.RangeAssignor")
              .setPartitionAssignment(PARTITION_ASSIGNMENT_1)
              .setState(State.STABLE)
              .setCoordinator(BROKER_1)
              .setConsumers(Arrays.asList(CONSUMER_LIST_1))
              .build(),
          ConsumerGroup.builder()
              .setClusterId(CLUSTER_ID)
              .setConsumerGroupId("consumer-group-2")
              .setSimple(false)
              .setPartitionAssignor("org.apache.kafka.clients.consumer.RoundRobinAssignor")
              .setPartitionAssignment(PARTITION_ASSIGNMENT_2)
              .setState(State.COMPLETING_REBALANCE)
              .setCoordinator(BROKER_2)
              .setConsumers(Arrays.asList(CONSUMER_LIST_2))
              .build()
      };

  @Rule
  public final EasyMockRule mocks = new EasyMockRule(this);

  @Mock
  private ConsumerGroupManager consumerGroupManager;

  private ConsumerGroupsResource consumerGroupsResource;

  @Before
  public void setUp() {
    consumerGroupsResource =
        new ConsumerGroupsResource(
            () -> consumerGroupManager, new CrnFactoryImpl(""), new FakeUrlFactory());
  }

  @Test
  public void listConsumerGroups_returnsConsumerGroups() {
    expect(consumerGroupManager.listConsumerGroups(CLUSTER_ID))
        .andReturn(completedFuture(Arrays.asList(CONSUMER_GROUPS)));
    replay(consumerGroupManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerGroupsResource.listConsumerGroups(response, CLUSTER_ID);

    ListConsumerGroupsResponse expected =
        ListConsumerGroupsResponse.create(
            ConsumerGroupDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf("/v3/clusters/cluster-1/consumer-groups")
                        .build())
                .setData(
                    Arrays.asList(
                        ConsumerGroupData.fromConsumerGroup(CONSUMER_GROUPS[0])
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-1")
                                    .build())
                            .setCoordinator(
                                Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                            .setConsumers(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                                        + "/consumers"))
                            .build(),
                        ConsumerGroupData.fromConsumerGroup(CONSUMER_GROUPS[1])
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-2")
                                    .setResourceName(
                                        "crn:///kafka=cluster-1/consumer-group=consumer-group-2")
                                    .build())
                            .setCoordinator(
                                Relationship.create("/v3/clusters/cluster-1/brokers/2"))
                            .setConsumers(
                                Relationship.create(
                                    "/v3/clusters/cluster-1/consumer-groups/consumer-group-2"
                                        + "/consumers"))
                            .build()))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumerGroup_returnsConsumerGroup() {
    expect(
        consumerGroupManager.getConsumerGroup(
            CLUSTER_ID, CONSUMER_GROUPS[0].getConsumerGroupId()))
        .andReturn(completedFuture(Optional.of(CONSUMER_GROUPS[0])));
    replay(consumerGroupManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerGroupsResource.getConsumerGroup(
        response, CLUSTER_ID, CONSUMER_GROUPS[0].getConsumerGroupId());

    GetConsumerGroupResponse expected =
        GetConsumerGroupResponse.create(
            ConsumerGroupData.fromConsumerGroup(CONSUMER_GROUPS[0])
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            "/v3/clusters/cluster-1/consumer-groups/consumer-group-1")
                        .setResourceName(
                            "crn:///kafka=cluster-1/consumer-group=consumer-group-1")
                        .build())
                .setCoordinator(
                    Relationship.create("/v3/clusters/cluster-1/brokers/1"))
                .setConsumers(
                    Relationship.create(
                        "/v3/clusters/cluster-1/consumer-groups/consumer-group-1"
                            + "/consumers"))
                .build());

    assertEquals(expected, response.getValue());
  }

  @Test
  public void getConsumerGroup_nonExistingConsumerGroup_throwsNotFound() {
    expect(
        consumerGroupManager.getConsumerGroup(
            CLUSTER_ID, CONSUMER_GROUPS[0].getConsumerGroupId()))
        .andReturn(completedFuture(Optional.empty()));
    replay(consumerGroupManager);

    FakeAsyncResponse response = new FakeAsyncResponse();
    consumerGroupsResource.getConsumerGroup(
        response, CLUSTER_ID, CONSUMER_GROUPS[0].getConsumerGroupId());

    assertEquals(NotFoundException.class, response.getException().getClass());
  }

  private static Partition createPartition(String topicName, int partitionId) {
    return Partition.create(
        CLUSTER_ID,
        topicName,
        partitionId,
        /* replicas= */ emptyList());
  }
}
