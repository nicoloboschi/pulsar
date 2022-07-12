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
package org.apache.pulsar.shell;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.shell.config.ConfigStore;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Parameters(commandDescription = "Manage Pulsar shell configurations.")
public class ConfigShell implements ShellCommandsProvider {

    @Getter
    @Parameters
    public static class Params {

        @Parameter(names = {"-h", "--help",}, help = true, description = "Show this help.")
        boolean help;
    }

    private interface RunnableWithResult {
        boolean run() throws Exception;
    }

    private JCommander jcommander;
    private Params params;
    private final PulsarShell pulsarShell;
    private final Map<String, RunnableWithResult> commands = new HashMap<>();
    private final ConfigStore configStore;
    private final ObjectMapper writer = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    @Getter
    private String currentConfig = ConfigStore.DEFAULT_CONFIG;

    public ConfigShell(PulsarShell pulsarShell) {
        this.configStore = pulsarShell.getConfigStore();
        this.pulsarShell = pulsarShell;
    }

    @Override
    public String getName() {
        return "config";
    }

    @Override
    public String getServiceUrl() {
        return null;
    }

    @Override
    public String getAdminUrl() {
        return null;
    }

    @Override
    public void setupState(Properties properties) {

        this.params = new Params();
        this.jcommander = new JCommander();
        jcommander.addObject(params);

        commands.put("list", new CmdConfigList());
        commands.put("put", new CmdConfigPut());
        commands.put("delete", new CmdConfigDelete());
        commands.put("use", new CmdConfigUse());
        commands.put("view", new CmdConfigView());
        commands.forEach((k, v) -> jcommander.addCommand(k, v));
    }

    @Override
    public void cleanupState(Properties properties) {
        setupState(properties);
    }

    @Override
    public JCommander getJCommander() {
        return jcommander;
    }

    @Override
    public boolean runCommand(String[] args) throws Exception {
        try {
            jcommander.parse(args);

            if (params.help) {
                jcommander.usage();
                return true;
            }

            String chosenCommand = jcommander.getParsedCommand();
            final RunnableWithResult command = commands.get(chosenCommand);
            if (command == null) {
                jcommander.usage();
                return false;
            }
            return command.run();
        } catch (Throwable e) {
            jcommander.getConsole().println(e.getMessage());
            String chosenCommand = jcommander.getParsedCommand();
            if (e instanceof ParameterException) {
                try {
                    jcommander.getUsageFormatter().usage(chosenCommand);
                } catch (ParameterException noCmd) {
                    e.printStackTrace();
                }
            } else {
                e.printStackTrace();
            }
            return false;
        }
    }

    @Parameters(commandDescription = "List configurations")
    private class CmdConfigList implements RunnableWithResult {

        @Override
        @SneakyThrows
        public boolean run() {
            print(configStore.listConfigs());
            return true;
        }
    }

    @Parameters(commandDescription = "Use the configuration for next commands")
    private class CmdConfigUse implements RunnableWithResult {
        @Parameter(description = "Name of the config", required = true)
        @JCommanderCompleter.ParameterCompleter(completer = JCommanderCompleter.ParameterCompleter.Completers.CONFIGS )
        private String name;

        @Override
        @SneakyThrows
        public boolean run() {
            final ConfigStore.ConfigEntry config = configStore.getConfig(name);
            if (config == null) {
                print("Config " + name + " not found");
                return false;
            }
            final String value = config.getValue();
            currentConfig = name;
            final Properties properties = new Properties();
            properties.load(new StringReader(value));
            pulsarShell.reload(properties);
            return true;
        }
    }

    @Parameters(commandDescription = "Show configuration")
    private class CmdConfigView implements RunnableWithResult {
        @Parameter(description = "Name of the config", required = true)
        @JCommanderCompleter.ParameterCompleter(completer = JCommanderCompleter.ParameterCompleter.Completers.CONFIGS )
        private String name;

        @Override
        @SneakyThrows
        public boolean run() {
            final ConfigStore.ConfigEntry config = configStore.getConfig(this.name);
            if (config == null) {
                print("Config " + name + " not found");
                return false;
            }
            print(config.getValue());
            return true;
        }
    }

    @Parameters(commandDescription = "Delete a configuration")
    private class CmdConfigDelete implements RunnableWithResult {
        @Parameter(description = "Name of the config", required = true)
        @JCommanderCompleter.ParameterCompleter(completer = JCommanderCompleter.ParameterCompleter.Completers.CONFIGS )
        private String name;

        @Override
        @SneakyThrows
        public boolean run() {
            if (currentConfig != null && currentConfig.equals(name)) {
                print("'" + name + "' is currenty used and it can't be deleted");
                return false;
            }
            configStore.deleteConfig(name);
            return true;
        }
    }

    @Parameters(commandDescription = "Put a new configuration or override an existing one.")
    private class CmdConfigPut implements RunnableWithResult {

        @Parameter(description = "Configuration name", required = true)
        private String name;

        @Parameter(names = {"--url"}, description = "URL of the config")
        private String url;

        @Parameter(names = {"--file"}, description = "File path of the config")
        @JCommanderCompleter.ParameterCompleter(completer = JCommanderCompleter.ParameterCompleter.Completers.FILES )
        private String file;

        @Override
        @SneakyThrows
        public boolean run() {
            byte[] bytes;

            if (file != null) {
                bytes = Files.readAllBytes(new File(file).toPath());
            } else if (url != null) {
                final ByteArrayOutputStream bout = new ByteArrayOutputStream();

                try (InputStream in = URI.create(url).toURL().openStream()) {
                    IOUtils.copy(in, bout);
                }
                bytes = bout.toByteArray();
            } else {
                print("--file or --url are required.");
                return false;
            }

            configStore.putConfig(new ConfigStore.ConfigEntry(name, new String(bytes, StandardCharsets.UTF_8)));
            return true;
        }
    }


    <T> void print(List<T> items) {
        for (T item : items) {
            print(item);
        }
    }

    <T> void print(T item) {
        try {
            if (item instanceof String) {
                jcommander.getConsole().println((String) item);
            } else {
                System.out.println(writer.writeValueAsString(item));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}