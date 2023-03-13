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

import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.proxy.server.mocks.BasicAuthentication;
import org.apache.pulsar.proxy.server.mocks.BasicAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxyRolesEnforcementTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyRolesEnforcementTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        conf.setBrokerClientAuthenticationParameters("authParam:broker");

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(BasicAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("test");
        Set<String> proxyRoles = new HashSet<>();
        proxyRoles.add("proxy");
        conf.setProxyRoles(proxyRoles);

        super.init();

        createAdminClient();
        producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testIncorrectRoles() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Step 1: Create Admin Client
        createAdminClient();

        // create a client which connects to proxy and pass authData
        String namespaceName = "my-property/my-ns";
        String topicName = "persistent://my-property/my-ns/my-topic1";
        String subscriptionName = "my-subscriber-name";
        String clientAuthParams = "authParam:client";
        String proxyAuthParams = "authParam:proxy";

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        boolean exceptionOccurred = false;
        // Step 2: Try to use proxy Client as a normal Client - expect exception
        try (PulsarClient proxyClient = createPulsarClient(pulsar.getBrokerServiceUrl(), proxyAuthParams)) {
            try {
                proxyClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
            } catch (Exception ex) {
                exceptionOccurred = true;
            }
            Assert.assertTrue(exceptionOccurred);
        }

        // Step 3: Run Pulsar Proxy and pass proxy params as client params - expect exception
        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());

        proxyConfig.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(proxyAuthParams);

        Set<String> providers = new HashSet<>();
        providers.add(BasicAuthenticationProvider.class.getName());
        proxyConfig.setAuthenticationProviders(providers);

        try (ProxyService proxyService = new ProxyService(proxyConfig)) {
            proxyService.start();


            try (PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), proxyAuthParams)) {
                exceptionOccurred = false;
                try {
                    proxyClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
                } catch (Exception ex) {
                    exceptionOccurred = true;
                }

                Assert.assertTrue(exceptionOccurred);
            }

            // Step 4: Pass correct client params
            try (PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), clientAuthParams)) {
                proxyClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
            }
        }
    }

    private void createAdminClient() throws PulsarClientException {
        String adminAuthParams = "authParam:admin";
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(BasicAuthentication.class.getName(), adminAuthParams).build());
    }

    private PulsarClient createPulsarClient(String proxyServiceUrl, String authParams) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(proxyServiceUrl).authentication(BasicAuthentication.class.getName(),
                authParams).build();
    }
}
