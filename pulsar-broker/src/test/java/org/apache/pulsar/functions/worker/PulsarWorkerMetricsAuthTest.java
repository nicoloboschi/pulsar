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
package org.apache.pulsar.functions.worker;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.internal.JacksonConfigurator;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.tls.NoopHostnameVerifier;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Slf4j
@Test(groups = "broker-admin")
public class PulsarWorkerMetricsAuthTest extends MockedPulsarServiceBaseTest {

    private static String getTLSFile(String name) {
        return String.format("./src/test/resources/authentication/tls-http/%s.pem", name);
    }

    WorkerConfig workerConfig;
    WorkerServer workerServer;
    PulsarFunctionTestTemporaryDirectory tempDirectory;
    String adminUrl;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(getTLSFile("broker.cert"));
        conf.setTlsKeyFilePath(getTLSFile("broker.key-pk8"));
        conf.setTlsTrustCertsFilePath(getTLSFile("ca.cert"));
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(
                Set.of("org.apache.pulsar.broker.authentication.AuthenticationProviderTls"));
        conf.setSuperUserRoles(Set.of("admin", "superproxy"));
        conf.setProxyRoles(Set.of("proxy", "superproxy"));
        conf.setAuthorizationEnabled(true);

        conf.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationTls");
        conf.setBrokerClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s", getTLSFile("admin.cert"), getTLSFile("admin.key-pk8")));
        conf.setBrokerClientTrustCertsFilePath(getTLSFile("ca.cert"));
        conf.setBrokerClientTlsEnabled(true);
        conf.setNumExecutorThreadPoolSize(5);

        super.internalSetup();

        PulsarAdmin admin = mock(PulsarAdmin.class);
        Tenants tenants = mock(Tenants.class);
        when(admin.tenants()).thenReturn(tenants);
        Set<String> admins = Sets.newHashSet("superUser", "admin");
        TenantInfoImpl tenantInfo = new TenantInfoImpl(admins, null);
        when(tenants.getTenantInfo(any())).thenReturn(tenantInfo);
        Namespaces namespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(namespaces);

        final WorkerStatsManager workerStatsManager = mock(WorkerStatsManager.class);
        final FunctionRuntimeManager functionRuntimeManager = mock(FunctionRuntimeManager.class);
        when(functionRuntimeManager.getFunctionRuntimeInfos()).thenReturn(Collections.emptyMap());
        when(functionRuntimeManager.getMyInstances()).thenReturn(0);

        final PulsarWorkerService functionsWorkerService = spy(createPulsarFunctionWorker(conf, admin));
        doNothing().when(functionsWorkerService).initAsStandalone(any(WorkerConfig.class));
        when(functionsWorkerService.getBrokerAdmin()).thenReturn(admin);
        when(functionsWorkerService.getFunctionRuntimeManager()).thenReturn(functionRuntimeManager);
        when(functionsWorkerService.getWorkerStatsManager()).thenReturn(workerStatsManager);
        functionsWorkerService.init(workerConfig, null, false);

        AuthenticationService authenticationService = new AuthenticationService(conf);
        AuthorizationService authorizationService = new AuthorizationService(conf, mock(PulsarResources.class));
        when(functionsWorkerService.getAuthenticationService()).thenReturn(authenticationService);
        when(functionsWorkerService.getAuthorizationService()).thenReturn(authorizationService);
        when(functionsWorkerService.isInitialized()).thenReturn(true);

        // mock: once authentication passes, function should return response: function already exist
        FunctionMetaDataManager dataManager = mock(FunctionMetaDataManager.class);
        when(dataManager.containsFunction(any(), any(), any())).thenReturn(true);
        when(functionsWorkerService.getFunctionMetaDataManager()).thenReturn(dataManager);

        workerServer = new WorkerServer(functionsWorkerService, authenticationService, authorizationService);
        workerServer.start();
        adminUrl = String.format("https://%s:%s",
                functionsWorkerService.getWorkerConfig().getWorkerHostname(), workerServer.getListenPortHTTPS().get());
    }


    private PulsarWorkerService createPulsarFunctionWorker(ServiceConfiguration config,
                                                           PulsarAdmin mockPulsarAdmin) {
        workerConfig = new WorkerConfig();
        tempDirectory = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
        tempDirectory.useTemporaryDirectoriesForWorkerConfig(workerConfig);
        // workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper()
                        .convertValue(new ThreadRuntimeFactoryConfig().setThreadGroupName("use"), Map.class));
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort().get());
        workerConfig.setPulsarWebServiceUrl("https://127.0.0.1:" + config.getWebServicePort().get());
        workerConfig.setFailureCheckFreqMs(100);
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setClusterCoordinationTopicName("coordinate");
        workerConfig.setFunctionAssignmentTopicName("assignment");
        workerConfig.setFunctionMetadataTopicName("metadata");
        workerConfig.setInstanceLivenessCheckFreqMs(100);
        workerConfig.setWorkerPort(null);
        workerConfig.setPulsarFunctionsCluster(config.getClusterName());
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setWorkerId("c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort());

        workerConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        workerConfig.setBrokerClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s", getTLSFile("admin.cert"), getTLSFile("admin.key-pk8")));
        workerConfig.setUseTls(true);
        workerConfig.setTlsAllowInsecureConnection(true);
        workerConfig.setTlsTrustCertsFilePath(getTLSFile("ca.cert"));

        workerConfig.setWorkerPortTls(0);
        workerConfig.setTlsEnabled(true);
        workerConfig.setTlsCertificateFilePath(getTLSFile("broker.cert"));
        workerConfig.setTlsKeyFilePath(getTLSFile("broker.key-pk8"));

        workerConfig.setAuthenticationEnabled(true);
        workerConfig.setAuthorizationEnabled(true);

        workerConfig.setSuperUserRoles(Sets.newHashSet("superUser", "admin"));
        workerConfig.setMetricsRoles(Sets.newHashSet("user1"));

        PulsarWorkerService workerService = new PulsarWorkerService(new PulsarWorkerService.PulsarClientCreator() {
            @Override
            public PulsarAdmin newPulsarAdmin(String pulsarServiceUrl, WorkerConfig workerConfig) {
                return mockPulsarAdmin;
            }

            @Override
            public PulsarClient newPulsarClient(String pulsarServiceUrl, WorkerConfig workerConfig) {
                return null;
            }
        });

        return workerService;
    }


    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        workerServer.stop();
    }

    WebTarget buildWebClient(String user) throws Exception {
        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);

        ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(httpConfig)
            .register(JacksonConfigurator.class).register(JacksonFeature.class);

        X509Certificate trustCertificates[] = SecurityUtility.loadCertificatesFromPemFile(
                getTLSFile("ca.cert"));
        SSLContext sslCtx = SecurityUtility.createSslContext(
                false, trustCertificates,
                SecurityUtility.loadCertificatesFromPemFile(getTLSFile(user + ".cert")),
                SecurityUtility.loadPrivateKeyFromPemFile(getTLSFile(user + ".key-pk8")));
        clientBuilder.sslContext(sslCtx).hostnameVerifier(NoopHostnameVerifier.INSTANCE);
        Client client = clientBuilder.build();

        return client.target(adminUrl);
    }

    PulsarAdmin buildAdminClient(String user) throws Exception {
        return PulsarAdmin.builder()
            .allowTlsInsecureConnection(false)
            .enableTlsHostnameVerification(false)
            .serviceHttpUrl(adminUrl)
            .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls",
                            String.format("tlsCertFile:%s,tlsKeyFile:%s",
                                          getTLSFile(user + ".cert"), getTLSFile(user + ".key-pk8")))
            .tlsTrustCertsFilePath(getTLSFile("ca.cert")).build();
    }

    @Test
    public void testWorkerMetricsStats() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.worker().getMetrics();
        }
        try (PulsarAdmin admin = buildAdminClient("user1")) {
            admin.worker().getMetrics();
        }
        try (PulsarAdmin admin = buildAdminClient("proxy")) {
            admin.worker().getMetrics();
            fail();
        } catch (PulsarAdminException.NotAuthorizedException ex) {
            assertTrue(ex.getMessage().contains("Forbidden"));
        }
    }


    @Test
    public void testWorkerFunctionsStats() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.worker().getFunctionsStats();
        }
        try (PulsarAdmin admin = buildAdminClient("user1")) {
            admin.worker().getFunctionsStats();
        }
        try (PulsarAdmin admin = buildAdminClient("proxy")) {
            admin.worker().getFunctionsStats();
            fail();
        } catch (PulsarAdminException.NotAuthorizedException ex) {
            assertTrue(ex.getMessage().contains("Forbidden"));
        }
    }

    @Test
    public void testWorkerMetrics() throws Exception {
        assertEquals(200, buildWebClient("admin")
                .path("/metrics")
                .request()
                .get().getStatus());
        assertEquals(200, buildWebClient("user1")
                .path("/metrics")
                .request()
                .get().getStatus());
        assertEquals(403, buildWebClient("proxy")
                .path("/metrics")
                .request()
                .get().getStatus());
    }

    @Test
    public void testWorkerInitialized() throws Exception {
        assertEquals(200, buildWebClient("admin")
                .path("/initialized")
                .request()
                .get().getStatus());
        assertEquals(200, buildWebClient("user1")
                .path("/initialized")
                .request()
                .get().getStatus());
        assertEquals(403, buildWebClient("proxy")
                .path("/initialized")
                .request()
                .get().getStatus());
    }

    @Test
    public void testWorkerVersion() throws Exception {
        assertEquals(200, buildWebClient("admin")
                .path("/version")
                .request()
                .get().getStatus());
        assertEquals(200, buildWebClient("user1")
                .path("/version")
                .request()
                .get().getStatus());
        assertEquals(403, buildWebClient("proxy")
                .path("/version")
                .request()
                .get().getStatus());
    }

}