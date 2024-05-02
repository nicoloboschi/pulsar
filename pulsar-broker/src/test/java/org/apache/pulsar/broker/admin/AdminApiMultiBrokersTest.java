/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test multi-broker admin api.
 */
@Slf4j
@Test(groups = "broker-admin")
public class AdminApiMultiBrokersTest extends MultiBrokerBaseTest {
    @Override
    protected int numberOfAdditionalBrokers() {
        return 2;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setManagedLedgerMaxEntriesPerLedger(10);
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
    }

    @Test(timeOut = 30 * 1000)
    public void testGetLeaderBroker()
            throws ExecutionException, InterruptedException, PulsarAdminException {
        List<PulsarService> allBrokers = getAllBrokers();
        Optional<LeaderBroker> leaderBroker =
                allBrokers.get(0).getLeaderElectionService().readCurrentLeader().get();
        assertTrue(leaderBroker.isPresent());
        log.info("Leader broker is {}", leaderBroker);
        for (PulsarAdmin admin : getAllAdmins()) {
            String brokerId = admin.brokers().getLeaderBroker().getBrokerId();
            assertEquals(leaderBroker.get().getBrokerId(), brokerId);
        }
    }

    /**
     * The data provider provide these data [TopicDomain, IsPartition].
     */
    @DataProvider
    public Object[][] topicTypes() {
        return new Object[][]{
                {TopicDomain.persistent, false},
                {TopicDomain.persistent, true},
                {TopicDomain.non_persistent, false},
                {TopicDomain.non_persistent, true},
        };
    }

    @Test(timeOut = 30 * 1000, dataProvider = "topicTypes")
    public void testTopicLookup(TopicDomain topicDomain, boolean isPartition) throws Exception {
        PulsarAdmin admin0 = getAllAdmins().get(0);

        String namespace = RandomStringUtils.randomAlphabetic(5);
        admin0.namespaces().createNamespace("public/" + namespace, 3);
        admin0.namespaces().setAutoTopicCreation("public/" + namespace,
                AutoTopicCreationOverride.builder().allowAutoTopicCreation(false).build());

        TopicName topic = TopicName.get(topicDomain.value(), "public", namespace, "t1");
        if (isPartition) {
            admin0.topics().createPartitionedTopic(topic.getPartitionedTopicName(), 3);
        } else {
            admin0.topics().createNonPartitionedTopic(topic.getPartitionedTopicName());
        }

        // To ensure all admin could get a right lookup result.
        Set<String> lookupResultSet = new HashSet<>();
        for (PulsarAdmin pulsarAdmin : getAllAdmins()) {
            try {
                if (isPartition) {
                    lookupResultSet.add(pulsarAdmin.lookups().lookupTopic(topic.getPartition(0).toString()));
                } else {
                    lookupResultSet.add(pulsarAdmin.lookups().lookupTopic(topic.getPartitionedTopicName()));
                }
            } catch (Exception e) {
                log.error(pulsarAdmin.getServiceUrl() + " - Failed to execute lookup for topic {} .", topic, e);
                Assert.fail("Failed to execute lookup by PulsarAdmin(" + pulsarAdmin.getServiceUrl() + ").");
            }
        }
        Assert.assertEquals(lookupResultSet.size(), 1);
    }

    @Test(groups = "flaky")
    public void testForceDeletePartitionedTopicWithSub() throws Exception {
        final int numPartitions = 10;
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("tenant-xyz", tenantInfo);
        admin.namespaces().createNamespace("tenant-xyz/ns-abc", Set.of("test"));

        admin.namespaces().setAutoTopicCreation("tenant-xyz/ns-abc",
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("partitioned")
                        .defaultNumPartitions(5)
                        .build());

        RetentionPolicies retention = new RetentionPolicies(10, 10);
        admin.namespaces().setRetention("tenant-xyz/ns-abc", retention);
        final String topic = "persistent://tenant-xyz/ns-abc/topic-"
                + RandomStringUtils.randomAlphabetic(5)
                + "-testDeletePartitionedTopicWithSub";
        final String subscriptionName = "sub";
        ((TopicsImpl) admin.topics()).createPartitionedTopicAsync(topic, numPartitions, true, null).get();

        log.info("Creating producer and consumer");
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).enableBatching(false).topic(topic).create();

        log.info("producing messages");
        for (int i = 0; i < numPartitions * 100; ++i) {
            producer.newMessage()
                    .key("" + i)
                    .value("value-" + i)
                    .send();
        }
        producer.flush();

        log.info("consuming some messages");
        for (int i = 0; i < numPartitions * 5; i++) {
            Message<byte[]> m = consumer.receive(1, TimeUnit.MINUTES);
        }

        log.info("trying to delete the topic {}", topic);
        admin.topics().deletePartitionedTopic(topic, true);

        log.info("closing producer and consumer");
        producer.close();
        consumer.close();

        // topic autocreate might sneak in after the delete / before close
        // but the topic metadata should be consistent to go through deletion again
        if (admin.topics().getList("tenant-xyz/ns-abc")
                .stream().anyMatch(t -> t.contains(topic))) {
            try {
                admin.topics().deletePartitionedTopic(topic, true);
            } catch (PulsarAdminException.NotFoundException nfe) {
                // pass
            }

            assertEquals(0, admin.topics().getList("tenant-xyz/ns-abc")
                    .stream().filter(t -> t.contains(topic)).count());
            assertEquals(0,
                    pulsar.getPulsarResources().getTopicResources()
                            .getExistingPartitions(TopicName.getPartitionedTopicName(topic))
                            .get()
                            .stream().filter(t -> t.contains(topic)).count());
            assertFalse(admin.topics()
                    .getPartitionedTopicList("tenant-xyz/ns-abc")
                    .contains(topic));
        } else {
            log.info("trying to create the topic again");
            ((TopicsImpl) admin.topics()).createPartitionedTopicAsync(topic, numPartitions, true, null).get();
        }
    }


    @Test
    public void testLB() throws Exception {
        PulsarAdmin admin0 = getAllAdmins().get(0);

//        String namespace = RandomStringUtils.randomAlphabetic(5);
//        admin0.namespaces().createNamespace("public/" + namespace, 3);
        String namespace2 = RandomStringUtils.randomAlphabetic(5);
        admin0.namespaces().createNamespace("public/" + namespace2, 3);

        TopicName otherTopic = TopicName.get(TopicDomain.persistent.value(), "public", namespace2, "my-other-topic");
        admin0.topics().createPartitionedTopic(otherTopic.getPartitionedTopicName(), 50);
        TopicName topic = TopicName.get(TopicDomain.persistent.value(), "public", namespace2, "my-good-topic");
        admin0.topics().createNonPartitionedTopic(topic.getPartitionedTopicName());


        List<String> allTopics = List.of(otherTopic.getPartitionedTopicName(), topic.getPartitionedTopicName());

        CompletableFuture.runAsync(() -> {
            while (true) {
                try {
                    TopicName too = TopicName.get(TopicDomain.persistent.value(), "public", namespace2, "my-other-topic" + UUID.randomUUID());
                    try {
                        admin0.topics().createPartitionedTopic(too.getPartitionedTopicName(), 3);
                        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(too.getPartitionedTopicName())
                                .producerName("test-prod")
                                .create();) {
                            for (int i = 0; i < 100; i++) {
                                producer.newMessage()
                                        .value("test".getBytes())
                                        .send();
                                Thread.sleep(10);
                            }
                            log.info("producer is done with {}", allTopic);
//                            return;
                        } catch (Throwable tt) {
                            log.error(tt.getMessage(), tt);
                        }
                    }
                    } catch (Throwable tt) {
                        log.error(tt.getMessage(), tt);
                    }

                }
            }});

            for (String allTopic : allTopics) {
//            for (int i = 0; i < 50; i++) {
                CompletableFuture.runAsync(() -> {
                    while (true) {

                        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(allTopic)
                                .producerName("test-prod")
                                .create();) {
                            for (int i = 0; i < 100; i++) {
                                producer.newMessage()
                                        .value("test".getBytes())
                                        .send();
                                Thread.sleep(10);
                            }
                            log.info("producer is done with {}", allTopic);
//                            return;
                        } catch (Throwable tt) {
                            log.error(tt.getMessage(), tt);
                        }
                    }
                });
//            }
//

//            CompletableFuture.runAsync(() -> {
//                while (true) {
//
//                    try (Consumer<byte[]> producer = pulsarClient.newConsumer().topic(allTopic)
//                            .subscriptionName("sub1").subscribe();) {
//                        while (true) {
//                            Message<byte[]> receive = producer.receive();
//                        }
//                    } catch (Throwable tt) {
//                    }
//                }
//            });

            }
            // To ensure all admin could get a right lookup result.
            while (true) {
                Thread.sleep(2000);
                String owner = getOwmer(topic.getPartitionedTopicName());
//            String owner1 = getOwmer("my-other-topic-partition-9");
//            String owner2 = getOwmer("my-other-topic-partition-0");
                if (owner != null) {
//                split(topic);
//                getAllAdmins().get(0).topics().unload(topic.getPartitionedTopicName());
//                getAllAdmins().get(0).topics().unload(otherTopic.getPartitionedTopicName());
                    split(otherTopic);
                } else {
                    log.info("no owner:");
                }
            }
        }

        @SneakyThrows
        private String getOwmer (String topicName) throws IOException, InterruptedException {
            String currentOwner = null;
            for (PulsarAdmin pulsarAdmin : getAllAdmins()) {
                String current = pulsarAdmin.getServiceUrl();
                String metrics = (String) HttpClient.newHttpClient().send(
                        HttpRequest.newBuilder(URI.create(current + "/metrics/"))
                                .GET()
                                .build(), HttpResponse.BodyHandlers.ofString()).body();
                List<String> lines = metrics.lines().toList();
                for (String line : lines) {
                    if (line.startsWith("pulsar_storage_write_latency_count{") && line.contains(topicName)) {
                        if (currentOwner != null) {
                            throw new RuntimeException("Found same metrics for both brokers:" + current + " vs " + currentOwner + " for topic " + topicName);
                        } else {
                            currentOwner = current;
                        }
                    }
                }
            }
            try {
                log.info("stats: {}", admin.topics().getStats(topicName).getPublishers());
            } catch (Exception e) {

            }
            log.info("[" + topicName + "] owner:" + currentOwner);
            return currentOwner;
        }

        private void split (TopicName topic) throws PulsarAdminException {
            BundlesData bundles = admin.namespaces().getBundles(topic.getNamespace());
            int numBundles = bundles.getNumBundles();
            var bundleRanges = bundles.getBoundaries().stream().map(Long::decode).sorted().toList();
            String firstBundle = bundleRanges.get(0) + "_" + bundleRanges.get(1);
            log.info("gonna split " + firstBundle);
            admin.namespaces().splitNamespaceBundle(topic.getNamespace(), firstBundle, true, "range_equally_divide");
        }


    }
