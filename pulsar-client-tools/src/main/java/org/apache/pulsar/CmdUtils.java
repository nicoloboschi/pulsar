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
package org.apache.pulsar;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.felix.service.command.Process;
import org.apache.pulsar.common.util.ObjectMapperFactory;

public class CmdUtils {

    private static final ObjectMapper mapper = ObjectMapperFactory.create();
    private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();

    public static <T> void print(List<T> items) {
        for (T item : items) {
            print(item);
        }
    }

    public static <K, V> void print(Map<K, V> items) {
        for (Map.Entry<K, V> entry : items.entrySet()) {
            print(entry.getKey() + "    " + entry.getValue());
        }
    }

    public static <T> void print(T item) {
        try {
            if (item instanceof String) {
                System.out.println(item);
            } else {
                System.out.println(writer.writeValueAsString(item));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static <T> T loadConfig(String file, Class<T> clazz) throws IOException {
        try {
            return ObjectMapperFactory.getThreadLocalYaml().readValue(new File(file), clazz);
        } catch (Exception ex) {
            if (ex instanceof UnrecognizedPropertyException) {
                UnrecognizedPropertyException unrecognizedPropertyException = (UnrecognizedPropertyException) ex;

                String exceptionMessage = String.format("Failed to parse config file %s. "
                                + "Invalid field '%s' on line: %d column: %d. Valid fields are %s",
                        file,
                        unrecognizedPropertyException.getPath().get(0).getFieldName(),
                        unrecognizedPropertyException.getLocation().getLineNr(),
                        unrecognizedPropertyException.getLocation().getColumnNr(),
                        unrecognizedPropertyException.getKnownPropertyIds());
                throw new ParameterException(exceptionMessage);
            } else if (ex instanceof InvalidFormatException) {

                InvalidFormatException invalidFormatException = (InvalidFormatException) ex;
                String exceptionMessage = String.format("Failed to parse config file %s. %s on line: %d column: %d",
                        file,
                        invalidFormatException.getOriginalMessage(),
                        invalidFormatException.getLocation().getLineNr(),
                        invalidFormatException.getLocation().getColumnNr());

                throw new ParameterException(exceptionMessage);
            } else {
                throw new ParameterException(ex.getMessage());
            }
        }
    }
}
