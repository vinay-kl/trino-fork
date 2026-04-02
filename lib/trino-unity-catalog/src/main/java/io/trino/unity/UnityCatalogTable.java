/*
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
package io.trino.unity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public record UnityCatalogTable(
        String name,
        @JsonProperty("catalog_name") String catalogName,
        @JsonProperty("schema_name") String schemaName,
        @JsonProperty("table_type") String tableType,
        @JsonProperty("data_source_format") String dataSourceFormat,
        @JsonProperty("storage_location") String storageLocation,
        @JsonProperty("table_id") String tableId,
        List<UnityCatalogColumn> columns,
        Map<String, String> properties,
        String comment,
        String owner)
{
    @JsonCreator
    public UnityCatalogTable {}

    public Optional<List<UnityCatalogColumn>> optionalColumns()
    {
        return Optional.ofNullable(columns);
    }

    public Optional<Map<String, String>> optionalProperties()
    {
        return Optional.ofNullable(properties);
    }

    public Optional<String> optionalComment()
    {
        return Optional.ofNullable(comment);
    }

    public Optional<String> optionalOwner()
    {
        return Optional.ofNullable(owner);
    }
}
