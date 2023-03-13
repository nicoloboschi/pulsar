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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Sets;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockAuthentication;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MetricsAuthenticationTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.auth.MockAuthenticationProvider"));
        conf.setSuperUserRoles(Sets.newHashSet("pass.super"));
        conf.setMetricsRoles(Sets.newHashSet("pass.metrics"));
        conf.setAuthorizationEnabled(true);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    void testAuthenticateMetrics() throws Exception {
        conf.setAuthenticateMetricsEndpoint(true);
        super.internalSetup();
        Assert.assertEquals(
                requestMetrics("fail.fail").getStatus(),
                Response.Status.UNAUTHORIZED.getStatusCode()
        );

        Assert.assertEquals(
                requestMetrics("pass.nometrics").getStatus(),
                Response.Status.FORBIDDEN.getStatusCode()
        );
        Assert.assertEquals(
                requestMetrics("pass.super").getStatus(),
                Response.Status.OK.getStatusCode()
        );
        Assert.assertEquals(
                requestMetrics("pass.metrics").getStatus(),
                Response.Status.OK.getStatusCode()
        );
    }

    @Test
    void testGetMetricsByDefault() throws Exception {
        super.internalSetup();
        Response r = requestMetrics("fail.fail");
        Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
    }

    private Response requestMetrics(String user) {
        @Cleanup
        Client client = javax.ws.rs.client.ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));

        return client
                .target(this.pulsar.getWebServiceAddress())
                .path("/metrics")
                .request()
                .header(MockAuthentication.HTTP_HEADER_USER, user)
                .get();
    }
}
