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
public record TemporaryCredentials(
        @JsonProperty("aws_temp_credentials") AwsTempCredentials awsTempCredentialsOrNull,
        @JsonProperty("azure_user_delegation_sas") AzureTempCredentials azureTempCredentialsOrNull,
        @JsonProperty("gcp_oauth_token") GcpTempCredentials gcpTempCredentialsOrNull,
        @JsonProperty("expiration_time") Object expirationTimeOrNull)
{
    @JsonCreator
    public TemporaryCredentials {}

    /**
     * Databricks UC returns expiration_time at the top level of the credential response
     * as an epoch milliseconds number (e.g., 1773166207000). Some implementations may also
     * include it within the per-cloud credential object. This method returns the top-level
     * value as a string for parsing.
     */
    public Optional<String> expirationTime()
    {
        if (expirationTimeOrNull == null) {
            return Optional.empty();
        }
        return Optional.of(String.valueOf(expirationTimeOrNull));
    }

    public Optional<AwsTempCredentials> awsTempCredentials()
    {
        return Optional.ofNullable(awsTempCredentialsOrNull);
    }

    public Optional<AzureTempCredentials> azureTempCredentials()
    {
        return Optional.ofNullable(azureTempCredentialsOrNull);
    }

    public Optional<GcpTempCredentials> gcpTempCredentials()
    {
        return Optional.ofNullable(gcpTempCredentialsOrNull);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AwsTempCredentials(
            @JsonProperty("access_key_id") String accessKeyId,
            @JsonProperty("secret_access_key") String secretAccessKey,
            @JsonProperty("session_token") String sessionToken,
            @JsonProperty("expiration_time") String expirationTime)
    {
        @JsonCreator
        public AwsTempCredentials {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AzureTempCredentials(
            @JsonProperty("sas_token") String sasToken,
            @JsonProperty("expiration_time") String expirationTime)
    {
        @JsonCreator
        public AzureTempCredentials {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GcpTempCredentials(
            @JsonProperty("oauth_token") String oauthToken,
            @JsonProperty("expiration_time") String expirationTime)
    {
        @JsonCreator
        public GcpTempCredentials {}
    }
}
