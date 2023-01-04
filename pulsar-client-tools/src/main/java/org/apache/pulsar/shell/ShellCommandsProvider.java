/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.felix.service.command.CommandSession;
import org.apache.felix.service.command.Function;
import org.apache.felix.service.command.Process;

/**
 * Commands provider for Pulsar shell.
 */
public interface ShellCommandsProvider extends Function {

    /**
     * Name of the commands. This will be used as program name.
     *
     * @return
     */
    String getName();

    /**
     * Current service url for connecting to brokers. If the provider doesn't need brokers connection
     * or the service url is not set it must return null.
     *
     * @return service url
     */
    String getServiceUrl();

    /**
     * Current admin url for connecting to pulsar admin. If the provider doesn't need brokers connection
     * or the admin url is not set it must return null.
     *
     * @return admin url
     */
    String getAdminUrl();

    /**
     * Init state before a command is executed.
     * If the implementing class rely on JCommander, it's suggested to not recycle JCommander
     * objects because they are meant to single-shot usage.
     *
     * @param properties
     */
    void setupState(Properties properties);

    /**
     * Cleanup state after a command is executed.
     * If the implementing class rely on JCommander, it's suggested to not recycle JCommander
     * objects because they are meant to single-shot usage.
     *
     * @param properties
     */
    void cleanupState(Properties properties);

    /**
     * Return JCommander instance, if exists.
     *
     * @return
     */
    JCommander getJCommander();

    /**
     * Run command for the passed args.
     *
     * @param args arguments for the command. Note that the first word of the user command is omitted.
     * @throws Exception if any error occurs. The shell session will not be closed.
     */
    boolean runCommand(String[] args) throws Exception;


    @Override
    default Object execute(CommandSession session, List<Object> arguments) throws Exception {
        final PrintStream oldOut = System.out;
        final PrintStream oldErr = System.err;
        final Process current = Process.Utils.current();
        System.setOut(current.out());
        System.setErr(current.err());
        try {
            final String[] objects =
                    arguments.stream().map(a -> a.toString()).collect(Collectors.toList()).toArray(new String[]{});
            if (!runCommand(objects)) {
                return false;
            }
            return true;
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
        }
    }
}
