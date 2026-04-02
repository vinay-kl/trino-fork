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

import java.util.Map;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public record UnityCatalogSchema(
        String name,
        @JsonProperty("catalog_name") String catalogName,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("schema_id") String schemaId,
        String comment,
        Map<String, String> properties,
        String owner)
{
    @JsonCreator
    public UnityCatalogSchema {}

    public Optional<String> optionalFullName()
    {
        return Optional.ofNullable(fullName);
    }

    public Optional<String> optionalSchemaId()
    {
        return Optional.ofNullable(schemaId);
    }

    public Optional<String> optionalComment()
    {
        return Optional.ofNullable(comment);
    }

    public Optional<Map<String, String>> optionalProperties()
    {
        return Optional.ofNullable(properties);
    }

    public Optional<String> optionalOwner()
    {
        return Optional.ofNullable(owner);
    }
}
