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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.http.client.HttpClient;

import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class UnityCatalogModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(UnityCatalogConfig.class);
        httpClientBinder(binder).bindHttpClient("unity-catalog", ForUnityCatalog.class);
    }

    @Provides
    @Singleton
    public UnityCatalogTokenProvider tokenProvider(UnityCatalogConfig config)
    {
        return switch (config.getAuthType()) {
            case STATIC -> new StaticTokenProvider(config.getStaticToken());
            case EXTRA_CREDENTIALS -> new ExtraCredentialsTokenProvider(
                    config.getExtraCredentialName(),
                    config.isFallbackToStaticToken() ? Optional.ofNullable(config.getStaticToken()) : Optional.empty(),
                    config.isValidateTokenIdentity(),
                    config.getTokenIdentityClaim());
            case OAUTH2 -> new OAuth2TokenProvider(
                    config.isFallbackToStaticToken() ? Optional.ofNullable(config.getStaticToken()) : Optional.empty(),
                    config.isValidateTokenIdentity(),
                    config.getTokenIdentityClaim());
        };
    }

    @Provides
    @Singleton
    public UnityCatalogClient unityCatalogClient(@ForUnityCatalog HttpClient httpClient, UnityCatalogConfig config)
    {
        return new UnityCatalogClient(httpClient, config.getServerUri());
    }
}
