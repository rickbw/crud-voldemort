/* Copyright 2014 Rick Warren
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package rickbw.crud.voldemort.config;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import rickbw.crud.util.Preconditions;

import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.TimeoutConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.serialization.SerializerFactory;


/**
 * An immutable relative of {@link ClientConfig} that implements
 * {@link #equals(Object)} and {@link #hashCode()}.
 */
public class ImmutableClientConfig extends SmartClientConfig {

    /**
     * Prevent mutation after the initial deep copy performed in super's
     * constructor. It's actually read before it's initialized.
     */
    private boolean locked = false;
    private final Set<URI> cachedBootstrapUris;


    public static Builder newBuilder() {
        return new Builder(SmartClientConfig.create());
    }

    public static Builder newBuilderFrom(final ClientConfig config) {
        return new Builder(SmartClientConfig.copyOf(config));
    }

    public static Builder newBuilderFrom(final Properties clientConfigProps) {
        return new Builder(SmartClientConfig.from(clientConfigProps));
    }

    public static Builder loadBuilderFrom(final File clientConfigPropsFile) {
        return new Builder(SmartClientConfig.loadFrom(clientConfigPropsFile));
    }

    public static ImmutableClientConfig copyOf(final ClientConfig source) {
        if (source instanceof ImmutableClientConfig) {
            return (ImmutableClientConfig) source;
        } else {
            return new ImmutableClientConfig(source);
        }
    }

    /**
     * An alternative to {@link #getBootstrapUrls()} that avoids a deep copy,
     * is type-safe, and provides additional methods. The resulting
     * {@code Set} is immutable.
     */
    public Set<URI> getBootstrapUris() {
        return this.cachedBootstrapUris;
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public SmartClientConfig enableDefaultClient(final boolean enable) {
        /* TODO: Use Lens pattern to support pseudo-mutability by returning
         * new objects.
         */
        checkLocked();
        return super.enableDefaultClient(enable);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setAsyncJobThreadPoolSize(final int asyncJobThreadPoolSize) {
        checkLocked();
        return super.setAsyncJobThreadPoolSize(asyncJobThreadPoolSize);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setAsyncMetadataRefreshInMs(
            final long asyncCheckMetadataIntervalMs) {
        checkLocked();
        return super.setAsyncMetadataRefreshInMs(asyncCheckMetadataIntervalMs);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public SmartClientConfig setBootstrapUrls(final List<String> bootstrapUrls) {
        checkLocked();
        return super.setBootstrapUrls(bootstrapUrls);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setBootstrapUrls(final String... bootstrapUrls) {
        checkLocked();
        return super.setBootstrapUrls(bootstrapUrls);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setClientContextName(final String clientContextName) {
        checkLocked();
        return super.setClientContextName(clientContextName);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setClientRegistryUpdateIntervalInSecs(
            final int clientRegistryRefrshIntervalInSecs) {
        checkLocked();
        return super.setClientRegistryUpdateIntervalInSecs(clientRegistryRefrshIntervalInSecs);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setClientZoneId(final int clientZoneId) {
        checkLocked();
        return super.setClientZoneId(clientZoneId);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setConnectionTimeout(final int connectionTimeout,
            final TimeUnit unit) {
        checkLocked();
        return super.setConnectionTimeout(connectionTimeout, unit);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setEnableJmx(final boolean enableJmx) {
        checkLocked();
        return super.setEnableJmx(enableJmx);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setEnableLazy(final boolean enableLazy) {
        checkLocked();
        return super.setEnableLazy(enableLazy);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setEnablePipelineRoutedStore(
            final boolean enablePipelineRoutedStore) {
        checkLocked();
        return super.setEnablePipelineRoutedStore(enablePipelineRoutedStore);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setFailureDetectorAsyncRecoveryInterval(
            final long failureDetectorAsyncRecoveryInterval) {
        checkLocked();
        return super
                .setFailureDetectorAsyncRecoveryInterval(failureDetectorAsyncRecoveryInterval);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setFailureDetectorBannagePeriod(
            final long failureDetectorBannagePeriod) {
        checkLocked();
        return super.setFailureDetectorBannagePeriod(failureDetectorBannagePeriod);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public SmartClientConfig setFailureDetectorCatastrophicErrorTypes(
            final List<String> failureDetectorCatastrophicErrorTypes) {
        checkLocked();
        return super.setFailureDetectorCatastrophicErrorTypes(failureDetectorCatastrophicErrorTypes);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setFailureDetectorImplementation(
            final String failureDetectorImplementation) {
        checkLocked();
        return super.setFailureDetectorImplementation(failureDetectorImplementation);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setFailureDetectorRequestLengthThreshold(
            final long failureDetectorRequestLengthThreshold) {
        checkLocked();
        return super.setFailureDetectorRequestLengthThreshold(failureDetectorRequestLengthThreshold);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setFailureDetectorThreshold(
            final int failureDetectorThreshold) {
        checkLocked();
        return super.setFailureDetectorThreshold(failureDetectorThreshold);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setFailureDetectorThresholdCountMinimum(
            final int failureDetectorThresholdCountMinimum) {
        checkLocked();
        return super.setFailureDetectorThresholdCountMinimum(failureDetectorThresholdCountMinimum);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setFailureDetectorThresholdInterval(
            final long failureDetectorThresholdIntervalMs) {
        checkLocked();
        return super.setFailureDetectorThresholdInterval(failureDetectorThresholdIntervalMs);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setMaxBootstrapRetries(final int maxBootstrapRetries) {
        checkLocked();
        return super.setMaxBootstrapRetries(maxBootstrapRetries);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setMaxConnectionsPerNode(final int maxConnectionsPerNode) {
        checkLocked();
        return super.setMaxConnectionsPerNode(maxConnectionsPerNode);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setMaxQueuedRequests(final int maxQueuedRequests) {
        checkLocked();
        return super.setMaxQueuedRequests(maxQueuedRequests);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setMaxThreads(final int maxThreads) {
        checkLocked();
        return super.setMaxThreads(maxThreads);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setMaxTotalConnections(final int maxTotalConnections) {
        checkLocked();
        return super.setMaxTotalConnections(maxTotalConnections);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setNodeBannagePeriod(final int nodeBannagePeriod,
            final TimeUnit unit) {
        checkLocked();
        return super.setNodeBannagePeriod(nodeBannagePeriod, unit);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setRequestFormatType(
            final RequestFormatType requestFormatType) {
        checkLocked();
        return super.setRequestFormatType(requestFormatType);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setRoutingTier(final RoutingTier routingTier) {
        checkLocked();
        return super.setRoutingTier(routingTier);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setRoutingTimeout(final int routingTimeout, final TimeUnit unit) {
        checkLocked();
        return super.setRoutingTimeout(routingTimeout, unit);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setSelectors(final int selectors) {
        checkLocked();
        return super.setSelectors(selectors);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setSerializerFactory(
            final SerializerFactory serializerFactory) {
        checkLocked();
        return super.setSerializerFactory(serializerFactory);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setSocketBufferSize(final int socketBufferSize) {
        checkLocked();
        return super.setSocketBufferSize(socketBufferSize);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setSocketKeepAlive(final boolean socketKeepAlive) {
        checkLocked();
        return super.setSocketKeepAlive(socketKeepAlive);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setSocketTimeout(final int socketTimeout, final TimeUnit unit) {
        checkLocked();
        return super.setSocketTimeout(socketTimeout, unit);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setSysEnableJmx(final boolean sysEnableJmx) {
        checkLocked();
        return super.setSysEnableJmx(sysEnableJmx);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setSysEnablePipelineRoutedStore(
            final boolean sysEnablePipelineRoutedStore) {
        checkLocked();
        return super.setSysEnablePipelineRoutedStore(sysEnablePipelineRoutedStore);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public ClientConfig setThreadIdleTime(final long threadIdleTime, final TimeUnit unit) {
        checkLocked();
        return super.setThreadIdleTime(threadIdleTime, unit);
    }

    /**
     * @throw {@link UnsupportedOperationException}
     */
    @Deprecated
    @Override
    public SmartClientConfig setTimeoutConfig(final TimeoutConfig tConfig) {
        checkLocked();
        return super.setTimeoutConfig(tConfig);
    }

    @Override
    /*package*/ ImmutableTimeoutConfig copyOfTimeoutConfig(final TimeoutConfig tConfig) {
        return ImmutableTimeoutConfig.copyOf(tConfig);
    }

    @Override
    /*package*/ List<String> copyOfBootstrapUrls(final List<String> bootstrapUrls) {
        return Collections.unmodifiableList(super.copyOfBootstrapUrls(bootstrapUrls));
    }

    @Override
    /*package*/ List<String> copyOfFailureDetectorCatastrophicErrorTypes(final List<String> errorTypes) {
        return Collections.unmodifiableList(super.copyOfFailureDetectorCatastrophicErrorTypes(errorTypes));
    }

    private ImmutableClientConfig(final ClientConfig config) {
        super(config);
        this.locked = true;

        final Set<URI> uris = new LinkedHashSet<>();  // preserve input order
        for (final String uriString : getBootstrapUrls()) {
            uris.add(URI.create(uriString));
        }
        this.cachedBootstrapUris = Collections.unmodifiableSet(uris);
    }

    private void checkLocked() {
        if (this.locked) {
            throw new UnsupportedOperationException();
        }
    }

    public static final class Builder {
        private final ClientConfig config;

        private Builder(final ClientConfig config) {
            this.config = Preconditions.checkNotNull(config);
        }

        public Builder setAsyncJobThreadPoolSize(final int asyncJobThreadPoolSize) {
            this.config.setAsyncJobThreadPoolSize(asyncJobThreadPoolSize);
            return this;
        }

        public Builder setAsyncMetadataRefreshInMs(
                final long asyncCheckMetadataIntervalMs) {
            this.config.setAsyncMetadataRefreshInMs(asyncCheckMetadataIntervalMs);
            return this;
        }

        public Builder setBootstrapUrls(final List<String> bootstrapUrls) {
            this.config.setBootstrapUrls(bootstrapUrls);
            return this;
        }

        public Builder setBootstrapUrls(final String... bootstrapUrls) {
            this.config.setBootstrapUrls(bootstrapUrls);
            return this;
        }

        public Builder setClientContextName(final String clientContextName) {
            this.config.setClientContextName(clientContextName);
            return this;
        }

        public Builder setClientRegistryUpdateIntervalInSecs(
                final int clientRegistryRefrshIntervalInSecs) {
            this.config.setClientRegistryUpdateIntervalInSecs(clientRegistryRefrshIntervalInSecs);
            return this;
        }

        public Builder setClientZoneId(final int clientZoneId) {
            this.config.setClientZoneId(clientZoneId);
            return this;
        }

        public Builder setConnectionTimeout(
                final int connectionTimeout,
                final TimeUnit unit) {
            this.config.setConnectionTimeout(connectionTimeout, unit);
            return this;
        }

        public Builder setJmxEnabled(final boolean enable) {
            this.config.setEnableJmx(enable);
            return this;
        }

        public Builder setLazyEnabled(final boolean enable) {
            this.config.setEnableLazy(enable);
            return this;
        }

        public Builder setPipelineRoutedStoreEnabled(final boolean enable) {
            this.config.setEnablePipelineRoutedStore(enable);
            return this;
        }

        public Builder setFailureDetectorAsyncRecoveryInterval(
                final long failureDetectorAsyncRecoveryInterval) {
            this.config.setFailureDetectorAsyncRecoveryInterval(failureDetectorAsyncRecoveryInterval);
            return this;
        }

        public Builder setFailureDetectorBannagePeriod(
                final long failureDetectorBannagePeriod) {
            this.config.setFailureDetectorBannagePeriod(failureDetectorBannagePeriod);
            return this;
        }

        public Builder setFailureDetectorCatastrophicErrorTypes(
                final List<String> failureDetectorCatastrophicErrorTypes) {
            this.config.setFailureDetectorCatastrophicErrorTypes(failureDetectorCatastrophicErrorTypes);
            return this;
        }

        public Builder setFailureDetectorImplementation(
                final String failureDetectorImplementation) {
            this.config.setFailureDetectorImplementation(failureDetectorImplementation);
            return this;
        }

        public Builder setFailureDetectorRequestLengthThreshold(
                final long failureDetectorRequestLengthThreshold) {
            this.config.setFailureDetectorRequestLengthThreshold(failureDetectorRequestLengthThreshold);
            return this;
        }

        public Builder setFailureDetectorThreshold(
                final int failureDetectorThreshold) {
            this.config.setFailureDetectorThreshold(failureDetectorThreshold);
            return this;
        }

        public Builder setFailureDetectorThresholdCountMinimum(
                final int failureDetectorThresholdCountMinimum) {
            this.config.setFailureDetectorThresholdCountMinimum(failureDetectorThresholdCountMinimum);
            return this;
        }

        public Builder setFailureDetectorThresholdInterval(
                final long failureDetectorThresholdIntervalMs) {
            this.config.setFailureDetectorThresholdInterval(failureDetectorThresholdIntervalMs);
            return this;
        }

        public Builder setMaxBootstrapRetries(final int maxBootstrapRetries) {
            this.config.setMaxBootstrapRetries(maxBootstrapRetries);
            return this;
        }

        public Builder setMaxConnectionsPerNode(final int maxConnectionsPerNode) {
            this.config.setMaxConnectionsPerNode(maxConnectionsPerNode);
            return this;
        }

        public Builder setMaxQueuedRequests(final int maxQueuedRequests) {
            this.config.setMaxQueuedRequests(maxQueuedRequests);
            return this;
        }

        public Builder setMaxThreads(final int maxThreads) {
            this.config.setMaxThreads(maxThreads);
            return this;
        }

        @Deprecated
        public Builder setMaxTotalConnections(final int maxTotalConnections) {
            this.config.setMaxTotalConnections(maxTotalConnections);
            return this;
        }

        @Deprecated
        public Builder setNodeBannagePeriod(
                final int nodeBannagePeriod,
                final TimeUnit unit) {
            this.config.setNodeBannagePeriod(nodeBannagePeriod, unit);
            return this;
        }

        public Builder setRequestFormatType(
                final RequestFormatType requestFormatType) {
            this.config.setRequestFormatType(requestFormatType);
            return this;
        }

        public Builder setRoutingTier(final RoutingTier routingTier) {
            this.config.setRoutingTier(routingTier);
            return this;
        }

        public Builder setRoutingTimeout(final int routingTimeout, final TimeUnit unit) {
            this.config.setRoutingTimeout(routingTimeout, unit);
            return this;
        }

        public Builder setSelectors(final int selectors) {
            this.config.setSelectors(selectors);
            return this;
        }

        public Builder setSerializerFactory(
                final SerializerFactory serializerFactory) {
            this.config.setSerializerFactory(serializerFactory);
            return this;
        }

        public Builder setSocketBufferSize(final int socketBufferSize) {
            this.config.setSocketBufferSize(socketBufferSize);
            return this;
        }

        public Builder setSocketKeepAlive(final boolean socketKeepAlive) {
            this.config.setSocketKeepAlive(socketKeepAlive);
            return this;
        }

        public Builder setSocketTimeout(final int socketTimeout, final TimeUnit unit) {
            this.config.setSocketTimeout(socketTimeout, unit);
            return this;
        }

        public Builder setSysJmxEnabled(final boolean enable) {
            this.config.setSysEnableJmx(enable);
            return this;
        }

        public Builder setSysPipelineRoutedStoreEnabled(final boolean enable) {
            this.config.setSysEnablePipelineRoutedStore(enable);
            return this;
        }

        @Deprecated
        public Builder setThreadIdleTime(final long threadIdleTime, final TimeUnit unit) {
            this.config.setThreadIdleTime(threadIdleTime, unit);
            return this;
        }

        public Builder setTimeoutConfig(final TimeoutConfig tConfig) {
            this.config.setTimeoutConfig(tConfig);
            return this;
        }

        public Builder setDefaultClientEnabled(final boolean enabled) {
            this.config.enableDefaultClient(enabled);
            return this;
        }

        public ImmutableClientConfig build() {
            return new ImmutableClientConfig(this.config);
        }
    }

}
