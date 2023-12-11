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
package com.exactpro.th2.codec.oracle.logminer

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.exactpro.th2.codec.oracle.logminer.cfg.LogMinerConfiguration
import com.google.auto.service.AutoService
import java.io.InputStream

@AutoService(IPipelineCodecFactory::class)
class TransformerFactory : IPipelineCodecFactory {
    override val settingsClass: Class<out LogMinerConfiguration>
        get() = LogMinerConfiguration::class.java

    override val protocols: Set<String>
        get() = PROTOCOLS

    override fun create(settings: IPipelineCodecSettings?): IPipelineCodec {
        if (settings is LogMinerConfiguration) {
            return LogMinerTransformer(settings)
        }
        throw IllegalArgumentException("Unexpected setting type: " + (settings?.javaClass ?: "null"))
    }

    override fun init(pipelineCodecContext: IPipelineCodecContext) {}
    override fun init(dictionary: InputStream) {}
    override fun close() {}

    companion object {
        val PROTOCOLS = setOf("csv", "oracle-log-miner")
        val AGGREGATED_PROTOCOL = PROTOCOLS.joinToString(",", "[", "]")
    }
}