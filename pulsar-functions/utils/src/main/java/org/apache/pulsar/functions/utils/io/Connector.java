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
package org.apache.pulsar.functions.utils.io;

import java.nio.file.Path;
import java.util.List;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;

public interface Connector {

    @Builder
    @Data
    class ConnectorFromArchive implements Connector {
        private Path archivePath;
        private List<ConfigFieldDefinition> sourceConfigFieldDefinitions;
        private List<ConfigFieldDefinition> sinkConfigFieldDefinitions;
        private ClassLoader classLoader;
        private ConnectorDefinition connectorDefinition;

        @Override
        public boolean isFromCatalogue() {
            return false;
        }

        @Override
        public String getFunctionsPodDockerImage() {
            return null;
        }
    }

    @Builder
    @Data
    class ConnectorFromCatalogue implements Connector {
        private ConnectorDefinition connectorDefinition;
        private String dockerImageName;

        @Override
        public Path getArchivePath() {
            throw new UnsupportedOperationException("Cannot get archive path from catalogue's connector");
        }

        @Override
        public List<ConfigFieldDefinition> getSourceConfigFieldDefinitions() {
            throw new UnsupportedOperationException("Cannot get source config fields from catalogue's connector");
        }

        @Override
        public List<ConfigFieldDefinition> getSinkConfigFieldDefinitions() {
            throw new UnsupportedOperationException("Cannot get sink config fields from catalogue's connector");
        }

        @Override
        public ClassLoader getClassLoader() {
            throw new UnsupportedOperationException("Cannot get class loader from catalogue's connector");
        }

        @Override
        public boolean isFromCatalogue() {
            return true;
        }

        @Override
        public String getFunctionsPodDockerImage() {
            return dockerImageName;
        }
    }


    Path getArchivePath();

    List<ConfigFieldDefinition> getSourceConfigFieldDefinitions();

    List<ConfigFieldDefinition> getSinkConfigFieldDefinitions();

    ClassLoader getClassLoader();

    ConnectorDefinition getConnectorDefinition();

    boolean isFromCatalogue();

    String getFunctionsPodDockerImage();
}
