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
package io.trino.plugin.deltalake.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

@JsonIgnoreProperties(ignoreUnknown = true)
public record VendedCredentialsHandle(
        @JsonProperty("catalogOwned") boolean catalogOwned,
        @JsonProperty("managed") boolean managed,
        @JsonProperty("tableLocation") String tableLocation,
        @JsonProperty("tableId") Optional<String> tableId,
        @JsonProperty("operationType") String operationType,
        @JsonProperty("vendedCredentials") Optional<FileSystemCredentials> vendedCredentials)
{
    public static final String READ = "READ";
    public static final String READ_WRITE = "READ_WRITE";
    public static final String PATH_CREATE_TABLE = "PATH_CREATE_TABLE";

    @JsonCreator
    public VendedCredentialsHandle(
            @JsonProperty("catalogOwned") boolean catalogOwned,
            @JsonProperty("managed") boolean managed,
            @JsonProperty("tableLocation") String tableLocation,
            @JsonProperty("tableId") Optional<String> tableId,
            @JsonProperty("operationType") String operationType,
            @JsonProperty("vendedCredentials") Optional<FileSystemCredentials> vendedCredentials)
    {
        this.catalogOwned = catalogOwned;
        this.managed = managed;
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.operationType = requireNonNullElse(operationType, READ);
        this.vendedCredentials = requireNonNull(vendedCredentials, "vendedCredentials is null");
    }

    public VendedCredentialsHandle withOperationType(String operationType)
    {
        // PATH_CREATE_TABLE credentials already grant write access — don't downgrade
        if (PATH_CREATE_TABLE.equals(this.operationType)) {
            return this;
        }
        return new VendedCredentialsHandle(catalogOwned, managed, tableLocation, tableId, operationType, vendedCredentials);
    }

    public static VendedCredentialsHandle empty(String tableLocation)
    {
        return new VendedCredentialsHandle(false, false, tableLocation, Optional.empty(), READ, Optional.empty());
    }

    public static VendedCredentialsHandle forPathCreate(String tableLocation)
    {
        return new VendedCredentialsHandle(true, false, tableLocation, Optional.empty(), PATH_CREATE_TABLE, Optional.empty());
    }

    public static VendedCredentialsHandle of(DeltaMetastoreTable table)
    {
        return new VendedCredentialsHandle(table.catalogOwned(), table.managed(), table.location(), table.tableId(), READ, Optional.empty());
    }
}
