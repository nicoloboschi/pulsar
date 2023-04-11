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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

@Slf4j
public class ConnectorsManager {

    @Getter
    private volatile Map<String, Connector> connectors;
    private final String narExtractionDirectory;

    public ConnectorsManager(WorkerConfig workerConfig) throws IOException {
        this(workerConfig.getConnectorsDirectory(), workerConfig.getNarExtractionDirectory(),
                workerConfig.getConnectorsCatalogueUrl());
    }

    public ConnectorsManager(String connectorsDirectory, String narExtractionDirectory,
                             String connectorsCatalogueUrl) throws IOException {
        this.narExtractionDirectory = narExtractionDirectory;
        this.connectors = Collections.synchronizedMap(ConnectorUtils
                .searchForConnectors(connectorsDirectory, narExtractionDirectory,
                        connectorsCatalogueUrl)
        );
    }

    public Connector getConnector(String connectorType) {
        return connectors.get(connectorType);
    }

    public Connector loadConnector(String connectorType, Function.FunctionDetails.ComponentType componentType) throws IOException{
        return connectors.compute(connectorType, (k, connector) -> {
            final ClassLoader classLoader = connector.getClassLoader();
            if (classLoader == null && connector.getDownloadPath() != null) {
                try {
                    final File localArchive = loadArchiveFromDownloadPath(connectorType, connector);
                    connector.setArchivePath(localArchive.toPath());
                    connector.setClassLoader(
                            FunctionCommon.getClassLoaderFromPackage(componentType, null, localArchive, narExtractionDirectory)
                    );

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return connector;
        });
    }

    public ConnectorDefinition getConnectorDefinition(String connectorType) {
        return connectors.get(connectorType).getConnectorDefinition();
    }

    public List<ConnectorDefinition> getConnectorDefinitions() {
        return connectors.values().stream().map(connector -> connector.getConnectorDefinition())
                .collect(Collectors.toList());
    }

    public Path getSourceArchive(String sourceType) {
        return connectors.get(sourceType).getArchivePath();
    }

    public List<ConfigFieldDefinition> getSourceConfigDefinition(String sourceType) throws IOException {
        return loadConnector(sourceType, Function.FunctionDetails.ComponentType.SOURCE).getSourceConfigFieldDefinitions();
    }

    public List<ConfigFieldDefinition> getSinkConfigDefinition(String sinkType) throws IOException {
        return loadConnector(sinkType, Function.FunctionDetails.ComponentType.SINK).getSinkConfigFieldDefinitions();
    }

    public Path getSinkArchive(String sinkType) {
        return connectors.get(sinkType).getArchivePath();
    }

    public void reloadConnectors(WorkerConfig workerConfig) throws IOException {
        connectors = ConnectorUtils
                .searchForConnectors(workerConfig.getConnectorsDirectory(), workerConfig.getNarExtractionDirectory(),
                        workerConfig.getConnectorsCatalogueUrl());
    }

    private File loadArchiveFromDownloadPath(String archiveName, Connector connector)
            throws IOException {
        try {
            File localArchive;
            if (connector.getDownloadPath().startsWith(Utils.HTTP)) {
                final File localCache = new File(narExtractionDirectory,
                        "downloads");
                localCache.mkdirs();
                localArchive = new File(localCache, archiveName);
                if (!localArchive.exists()) {
                    localArchive = FunctionCommon.extractFileFromPkgURL(connector.getDownloadPath(), localArchive);
                }
            } else {
                localArchive = FunctionCommon.extractFileFromPkgURL(connector.getDownloadPath());
            }
            return localArchive;
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
