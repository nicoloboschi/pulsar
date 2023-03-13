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

import static org.mockito.Mockito.doReturn;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import java.util.Optional;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyStatsDisabledTest extends ProducerConsumerBase {

    private ProxyService proxyService;
    private WebServer proxyWebServer;
    private Client httpClient;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setClusterName("test");
        super.init();
        ProxyConfiguration proxyConfig = new ProxyConfiguration();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setProxyLogLevel(Optional.of(2));
        proxyConfig.setExposeProxyStatsEndpoint(false);
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));


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
    public void testEndpoints() throws Exception {
        Assert.assertEquals(httpBuilder("/proxy-stats").get()
                .getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(httpBuilder("/proxy-stats/connections").get()
                .getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(httpBuilder("/proxy-stats/topics").get()
                .getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(httpBuilder("/proxy-stats/logging").get()
                .getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    private Invocation.Builder httpBuilder(String path) {
        return httpClient
                .target(proxyWebServer.getServiceUri())
                .path(path)
                .request();
    }
}
