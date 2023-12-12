/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.codec.oracle.logminer.cfg

import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.MAPPER
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LogMinerLogMinerConfigurationTest {
    @Test
    fun deserializeTabAsDelimiter() {
        val value = """
            {
              "column-prefix": "my_prefix_",
              "save-columns": ["A", "B"]
            }
            """
        val cfg: LogMinerConfiguration = MAPPER.readValue(value, LogMinerConfiguration::class.java)
        assertEquals("my_prefix_", cfg.columnPrefix)
        assertEquals(setOf("A", "B"), cfg.saveColumns)
    }
}