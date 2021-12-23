/**
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
package org.apache.pulsar.io.flume.node;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public final class TestEnvVarResolverProperties {
    private static final File TESTFILE = new File(
            TestEnvVarResolverProperties.class.getClassLoader()
                    .getResource("flume-conf-with-envvars.properties").getFile());

    private PropertiesFileConfigurationProvider provider;

    @Before
    public void setUp() {
        provider = new PropertiesFileConfigurationProvider("a1", TESTFILE);
    }

    @Test
    public void resolveEnvVar() {

        injectEnvironmentVariable("VARNAME", "varvalue");
        String resolved = EnvVarResolverProperties.resolveEnvVars("padding ${VARNAME} padding");
        Assert.assertEquals("padding varvalue padding", resolved);
    }

    @Test
    public void resolveEnvVars() {
        injectEnvironmentVariable("VARNAME1", "varvalue1");
        injectEnvironmentVariable("VARNAME2", "varvalue2");
        String resolved = EnvVarResolverProperties
                .resolveEnvVars("padding ${VARNAME1} ${VARNAME2} padding");
        Assert.assertEquals("padding varvalue1 varvalue2 padding", resolved);
    }

    @Test
    public void getProperty() {
        String NC_PORT = "6667";
        injectEnvironmentVariable("NC_PORT", NC_PORT);
        System.setProperty("propertiesImplementation",
                "org.apache.pulsar.io.flume.node.EnvVarResolverProperties");

        Assert.assertEquals(NC_PORT, provider.getFlumeConfiguration()
                .getConfigurationFor("a1")
                .getSourceContext().get("r1").getParameters().get("port"));
    }

    @SneakyThrows
    private static void injectEnvironmentVariable(String key, String value) {

        Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
        Map<String,String> unmodifiableMap = new HashMap<>((Map<String,String>)
                Whitebox.getInternalState(processEnvironment, "theUnmodifiableEnvironment"));
        unmodifiableMap.put(key, value);
        Whitebox.setInternalState(processEnvironment, "theUnmodifiableEnvironment", unmodifiableMap);

        Map<String,String> envMap = new HashMap<>((Map<String,String>)
                Whitebox.getInternalState(processEnvironment, "theEnvironment"));
        envMap.put(key, value);
        Whitebox.setInternalState(processEnvironment, "theEnvironment", envMap);
    }
}
