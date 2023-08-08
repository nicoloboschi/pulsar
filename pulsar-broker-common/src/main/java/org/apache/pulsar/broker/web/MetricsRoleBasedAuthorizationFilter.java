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
package org.apache.pulsar.broker.web;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet filter that authorize the request only if the client role is a metric role or a super user.
 */
public class MetricsRoleBasedAuthorizationFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsRoleBasedAuthorizationFilter.class);

    private final AuthorizationService authorizationService;

    public MetricsRoleBasedAuthorizationFilter(AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    protected String getAuthenticatedRole(HttpServletRequest request) {
        return (String) request
                .getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
    }

    protected AuthenticationDataSource getAuthenticatedDataSource(HttpServletRequest request) {
        return (AuthenticationDataSource) request
                .getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        try {
            final HttpServletRequest httpRequest = (HttpServletRequest) request;
            final String role = getAuthenticatedRole(httpRequest);
            final AuthenticationDataSource authenticatedDataSource = getAuthenticatedDataSource(httpRequest);

            boolean authorized = authorizationService.allowToScrapeMetrics(role, authenticatedDataSource)
                    .join();
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Role {} authorized to scrape metrics: {}", httpRequest.getRemoteAddr(), role, authorized);
            }
            if (authorized) {
                chain.doFilter(request, response);
            } else {
                HttpServletResponse httpResponse = (HttpServletResponse) response;
                httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
                LOG.warn("[{}] Failed to authorize HTTP request, role {} is not allowed for uri {}",
                        request.getRemoteAddr(),
                        role,
                        httpRequest.getRequestURI());
            }
        } catch (Exception e) {
            HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
            LOG.error("[{}] Error performing authorization for HTTP", request.getRemoteAddr(), e);
        }
    }

    @Override
    public void init(FilterConfig arg) throws ServletException {
        // No init necessary.
    }

    @Override
    public void destroy() {
        // No state to clean up.
    }
}
