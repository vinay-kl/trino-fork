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

import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public record UnityCatalogColumn(
        String name,
        @JsonProperty("type_text") String typeText,
        @JsonProperty("type_name") String typeName,
        int position,
        String comment,
        boolean nullable)
{
    @JsonCreator
    public UnityCatalogColumn {}

    public Optional<String> optionalTypeText()
    {
        return Optional.ofNullable(typeText);
    }

    public Optional<String> optionalTypeName()
    {
        return Optional.ofNullable(typeName);
    }

    public Optional<String> optionalComment()
    {
        return Optional.ofNullable(comment);
    }
}
