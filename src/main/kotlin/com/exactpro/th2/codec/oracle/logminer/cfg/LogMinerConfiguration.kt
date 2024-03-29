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

import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.exactpro.th2.codec.oracle.logminer.LogMinerTransformer.Companion.REQUIRED_COLUMNS
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription

class LogMinerConfiguration : IPipelineCodecSettings {

    @JsonProperty("column-prefix")
    @JsonPropertyDescription("Codec uses this prefix for extracted columns")
    var columnPrefix: String = "th2_"

    @JsonProperty("save-columns")
    @JsonPropertyDescription("Codec saves: extracted columns and this set")
    var saveColumns: Set<String> = REQUIRED_COLUMNS

    @JsonProperty("truncate-update-query-from-where-clause")
    @JsonPropertyDescription("Codec truncates the tail of UPDATE query starting from the WHERE clause")
    var truncateUpdateQueryFromWhereClause: Boolean = true

    @JsonProperty("trim-parsed-content")
    @JsonPropertyDescription("Codec trims values parsed from SQL_REDO field")
    var trimParsedContent: Boolean = true
}
