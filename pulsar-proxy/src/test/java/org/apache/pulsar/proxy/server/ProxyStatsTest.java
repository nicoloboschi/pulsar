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
package org.apache.pulsar.proxy.server;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertNotNull;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.proxy.server.mocks.BasicAuthentication;
import org.apache.pulsar.proxy.server.mocks.BasicAuthenticationData;
import org.apache.pulsar.proxy.server.mocks.BasicAuthenticationProvider;
import org.apache.pulsar.proxy.stats.ConnectionStats;
import org.apache.pulsar.proxy.stats.TopicStats;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public abstract class ProxyStatsTest extends ProducerConsumerBase {

    private ProxyService proxyService;
    private WebServer proxyWebServer;
    private Client httpClient;

    private final boolean authEnabled;

    public ProxyStatsTest(boolean authEnabled) {
        this.authEnabled = authEnabled;
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        if (authEnabled) {
            conf.setAuthenticationEnabled(true);
            conf.setAuthorizationEnabled(true);
            conf.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
            conf.setBrokerClientAuthenticationParameters("authParam:admin");
            conf.setAuthenticateOriginalAuthData(false);


            Set<String> superUserRoles = new HashSet<String>();
            superUserRoles.add("admin");
            superUserRoles.add("proxy");
            conf.setSuperUserRoles(superUserRoles);

            Set<String> providers = new HashSet<String>();
            providers.add(BasicAuthenticationProvider.class.getName());
            conf.setAuthenticationProviders(providers);
            Set<String> proxyRoles = new HashSet<String>();
            proxyRoles.add("proxy");
            conf.setProxyRoles(proxyRoles);
        }
        conf.setClusterName("test");

        super.init();


        if (authEnabled) {
            String adminAuthParams = "authParam:admin";
            admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                    .authentication(BasicAuthentication.class.getName(), adminAuthParams).build());
        }
        producerBaseSetup();


        if (authEnabled) {

            String namespaceName = "my-property/my-ns";
            admin.namespaces().grantPermissionOnNamespace(namespaceName, "client",
                    Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        }

        ProxyConfiguration proxyConfig = new ProxyConfiguration();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setProxyLogLevel(Optional.of(2));
        if (authEnabled) {
            proxyConfig.setSuperUserRoles(Set.of("admin"));
            proxyConfig.setAuthenticationEnabled(true);
            proxyConfig.setAuthorizationEnabled(true);
            proxyConfig.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
            String proxyAuthParams = "authParam:proxy";
            proxyConfig.setBrokerClientAuthenticationParameters(proxyAuthParams);
            Set<String> providers = new HashSet<String>();
            providers.add(BasicAuthenticationProvider.class.getName());
            proxyConfig.setAuthenticationProviders(providers);
            proxyConfig.setForwardAuthorizationCredentials(true);
        }

        proxyService = Mockito.spy(new ProxyService(proxyConfig));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();
        proxyService.start();

        proxyWebServer = new WebServer(proxyService);
        ProxyServiceStarter.addWebServerHandlers(proxyWebServer, proxyConfig, proxyService, null);
        proxyWebServer.start();

        httpClient = ClientBuilder
                .newClient(new ClientConfig().register(LoggingFeature.class));

    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        // wait for shutdown of the broker, this prevents flakiness which could be caused by metrics being
        // unregistered asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        proxyService.close();
        httpClient.close();
    }

    @Test
    public void testConnectionsStats() throws Exception {
        String topicName1 = "persistent://my-property/my-ns/my-topic1";

        @Cleanup
        PulsarClient client = createPulsarClient();
        Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(topicName1).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(topicName1).subscriptionName("my-sub").subscribe();

        int totalMessages = 10;
        for (int i = 0; i < totalMessages; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < totalMessages; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            requireNonNull(msg);
            consumer.acknowledge(msg);
        }
        if (authEnabled) {
            assertGetConnectionStats(null, false);
            assertGetConnectionStats("notexists", false);
            assertGetConnectionStats("client", false);
            assertGetConnectionStats("admin", true);
        } else {
            assertGetConnectionStats(null, true);
        }
    }

    private void assertGetConnectionStats(String role, boolean expectOk) {
        if (!expectOk) {
            Assert.assertEquals(httpBuilder("/proxy-stats/connections", role).get()
                    .getStatus(), Response.Status.FORBIDDEN.getStatusCode());
        } else {
            Response r = httpBuilder("/proxy-stats/connections", role).get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            String response = r.readEntity(String.class).trim();
            List<ConnectionStats> connectionStats = new Gson().fromJson(response, new TypeToken<List<ConnectionStats>>() {
            }.getType());
            assertNotNull(connectionStats);
        }
    }

    @Test
    public void testTopicStats() throws Exception {
        proxyService.setProxyLogLevel(2);
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final String topicName2 = "persistent://my-property/my-ns/my-topic2";

        @Cleanup
        PulsarClient client = createPulsarClient();
        @Cleanup
        Producer<byte[]> producer1 = client.newProducer(Schema.BYTES).topic(topicName).enableBatching(false)
                .producerName("producer1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        @Cleanup
        Producer<byte[]> producer2 = client.newProducer(Schema.BYTES).topic(topicName2).enableBatching(false)
                .producerName("producer2").messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();
        @Cleanup
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topicName2).subscriptionName("my-sub")
                .subscribe();

        int totalMessages = 10;
        for (int i = 0; i < totalMessages; i++) {
            producer1.send("test".getBytes());
            producer2.send("test".getBytes());
        }

        for (int i = 0; i < totalMessages; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            requireNonNull(msg);
            consumer.acknowledge(msg);
            msg = consumer2.receive(1, TimeUnit.SECONDS);
        }
        if (authEnabled) {
            assertGetTopicsStats(topicName, null, false);
            assertGetTopicsStats(topicName, "notexists", false);
            assertGetTopicsStats(topicName, "client", false);
            assertGetTopicsStats(topicName, "admin", true);
        } else {
            assertGetTopicsStats(topicName, null, true);
        }
    }

    private void assertGetTopicsStats(String topicName, String role, boolean expectOk) {
        if (!expectOk) {
            Assert.assertEquals(httpBuilder("/proxy-stats/topics", role).get()
                    .getStatus(), Response.Status.FORBIDDEN.getStatusCode());

        } else {
            Response r = httpBuilder("/proxy-stats/topics", role).get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
            String response = r.readEntity(String.class).trim();
            Map<String, TopicStats> topicStats = new Gson().fromJson(response, new TypeToken<Map<String, TopicStats>>() {
            }.getType());

            assertNotNull(topicStats.get(topicName));
        }
    }

    @Test
    public void testChangeLogLevel() {
        Assert.assertEquals(proxyService.getProxyLogLevel(), 2);
        int newLogLevel = 1;

        if (authEnabled) {
            assertUpdateLogLevel(newLogLevel, null, false);
            assertUpdateLogLevel(newLogLevel, "notexists", false);
            assertUpdateLogLevel(newLogLevel, "client", false);
            assertUpdateLogLevel(newLogLevel, "admin", true);

            assertGetLogLevel(newLogLevel, null, false);
            assertGetLogLevel(newLogLevel, "notexists", false);
            assertGetLogLevel(newLogLevel, "client", false);
            assertGetLogLevel(newLogLevel, "admin", true);
        } else {
            assertUpdateLogLevel(newLogLevel, null, true);
            assertGetLogLevel(newLogLevel, null, true);
        }
    }

    private void assertGetLogLevel(int expectedLevel, String role, boolean expectOk) {
        if (!expectOk) {
            Assert.assertEquals(httpBuilder("/proxy-stats/logging", role).get()
                    .getStatus(), Response.Status.FORBIDDEN.getStatusCode());
        } else {
            final Response getResponse = httpBuilder("/proxy-stats/logging", role)
                    .get();
            Assert.assertEquals(getResponse.getStatus(), Response.Status.OK.getStatusCode());
            Assert.assertEquals(getResponse.readEntity(int.class), expectedLevel);
        }
    }

    private void assertUpdateLogLevel(int newLogLevel, String role, boolean expectOk) {
        final String postUrl = "/proxy-stats/logging/" + newLogLevel;
        if (!expectOk) {
            Assert.assertEquals(httpBuilder(postUrl, role)
                    .post(Entity.entity("", MediaType.APPLICATION_JSON))
                    .getStatus(), Response.Status.FORBIDDEN.getStatusCode());
            Assert.assertNotEquals(proxyService.getProxyLogLevel(), newLogLevel);
        } else {
            Response r = httpBuilder(postUrl, role)
                    .post(Entity.entity("", MediaType.APPLICATION_JSON));
            Assert.assertEquals(r.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
            Assert.assertEquals(proxyService.getProxyLogLevel(), newLogLevel);
        }
    }


    private Invocation.Builder httpBuilder(String path, String role) {
        BasicAuthenticationData auth =
                new BasicAuthenticationData(role);
        final Invocation.Builder builder = httpClient
                .target(proxyWebServer.getServiceUri())
                .path(path)
                .request();

        Set<Map.Entry<String, String>> headers = auth.getHttpHeaders();
        if (headers != null) {
            headers.forEach(entry -> builder.header(entry.getKey(), entry.getValue()));
        }
        return builder;
    }



    private PulsarClient createPulsarClient() throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .authentication(BasicAuthentication.class.getName(), "authParam:client").build();
    }

}
