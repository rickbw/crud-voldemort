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
package crud.voldemort.config;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import voldemort.client.ClientConfig;
import voldemort.client.TimeoutConfig;


/**
 * A subclass of {@link ClientConfig} that provides deep copying,
 * stringification, and equality. It can otherwise be used interchangeably
 * with its superclass.
 */
public class SmartClientConfig extends ClientConfig {

    public static SmartClientConfig create() {
        return new SmartClientConfig();
    }

    public static SmartClientConfig from(final Properties properties) {
        return new SmartClientConfig(properties);
    }

    public static SmartClientConfig loadFrom(final File propertiesFile) {
        return new SmartClientConfig(propertiesFile);
    }

    public static SmartClientConfig copyOf(final ClientConfig config) {
        return new SmartClientConfig(config);
    }

    /**
     * An alternative to {@link #enableDefaultClient(boolean)} that behaves
     * like a JavaBean property.
     *
     * @see #isDefaultClientEnabled()
     * @see #enableDefaultClient(boolean)
     */
    // TODO: Override all setters to return SmartClientConfig
    public SmartClientConfig setDefaultClientEnabled(final boolean enable) {
        super.enableDefaultClient(enable);
        return this;
    }

    /**
     * @deprecated  Prefer {@link #setDefaultClientEnabled(boolean)}: tools
     *              are more likely to recognize it as a setter.
     */
    @Deprecated
    @Override
    public SmartClientConfig enableDefaultClient(final boolean enable) {
        super.enableDefaultClient(enable);
        return this;
    }

    /**
     * This implementation behaves the same as the inherited one, except that
     * it always makes a deep copy of the input value.
     */
    @Override
    public SmartClientConfig setTimeoutConfig(final TimeoutConfig tConfig) {
        super.setTimeoutConfig(copyOfTimeoutConfig(tConfig));
        return this;
    }

    /**
     * This implementation behaves the same as the inherited one, except that
     * it always makes a deep copy of the input value.
     */
    @Override
    public SmartClientConfig setBootstrapUrls(final List<String> bootstrapUrls) {
        super.setBootstrapUrls(copyOfBootstrapUrls(bootstrapUrls));
        return this;
    }

    /**
     * This implementation behaves the same as the inherited one, except that
     * it always makes a deep copy of the input value.
     */
    @Override
    public SmartClientConfig setFailureDetectorCatastrophicErrorTypes(final List<String> errorTypes) {
        final List<String> copy = copyOfFailureDetectorCatastrophicErrorTypes(errorTypes);
        super.setFailureDetectorCatastrophicErrorTypes(copy);
        return this;
    }

    @Override
    public String toString() {
        @SuppressWarnings("deprecation")
        final int connections = getMaxTotalConnections();
        return getClass().getSimpleName()
                + " [asyncJobThreadPoolSize=" + getAsyncJobThreadPoolSize()
                + ", asyncMetadataRefreshInMs=" + getAsyncMetadataRefreshInMs()
                + ", bootstrapUrls=" + Arrays.toString(getBootstrapUrls())
                + ", clientContextName=" + getClientContextName()
                + ", clientRegistryUpdateIntervalInSecs=" + getClientRegistryUpdateIntervalInSecs()
                + ", clientZoneId=" + getClientZoneId()
                + ", defaultClientEnabled=" + isDefaultClientEnabled()
                + ", failureDetectorAsyncRecoveryInterval=" + getFailureDetectorAsyncRecoveryInterval()
                + ", failureDetectorBannagePeriod=" + getFailureDetectorBannagePeriod()
                + ", failureDetectorCatastrophicErrorTypes=" + getFailureDetectorCatastrophicErrorTypes()
                + ", failureDetectorImplementation=" + getFailureDetectorImplementation()
                + ", failureDetectorRequestLengthThreshold=" + getFailureDetectorRequestLengthThreshold()
                + ", failureDetectorThreshold=" + getFailureDetectorThreshold()
                + ", failureDetectorThresholdCountMinimum=" + getFailureDetectorThresholdCountMinimum()
                + ", failureDetectorThresholdInterval=" + getFailureDetectorThresholdInterval()
                + ", jmxEnabled=" + isJmxEnabled()
                + ", lazyEnabled=" + isLazyEnabled()
                + ", maxBootstrapRetries=" + getMaxBootstrapRetries()
                + ", maxConnectionsPerNode=" + getMaxConnectionsPerNode()
                + ", maxQueuedRequests=" + getMaxQueuedRequests()
                + ", maxThreads=" + getMaxThreads()
                + ", maxTotalConnections=" + connections
                + ", pipelineRoutedStoreEnabled=" + isPipelineRoutedStoreEnabled()
                + ", requestFormatType=" + getRequestFormatType()
                + ", routingTier=" + getRoutingTier()
                + ", selectors=" + getSelectors()
                + ", serializerFactory=" + getSerializerFactory()
                + ", socketBufferSize=" + getSocketBufferSize()
                + ", socketKeepAlive=" + getSocketKeepAlive()
                + ", sysConnectionTimeout=" + getSysConnectionTimeout()
                + ", sysEnableJmx=" + getSysEnableJmx()
                + ", sysEnablePipelineRoutedStore=" + getSysEnablePipelineRoutedStore()
                + ", sysMaxConnectionsPerNode=" + getSysMaxConnectionsPerNode()
                + ", sysRoutingTimeout=" + getSysRoutingTimeout()
                + ", sysSocketTimeout=" + getSysSocketTimeout()
                + ", timeoutConfig=" + getTimeoutConfig()
                + "]";
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SmartClientConfig other = (SmartClientConfig) obj;
        if (getAsyncJobThreadPoolSize() != other.getAsyncJobThreadPoolSize()) {
            return false;
        }
        if (!Arrays.equals(getBootstrapUrls(), other.getBootstrapUrls())) {
            return false;
        }
        if (!getClientContextName().equals(other.getClientContextName())) {
            return false;
        }
        if (getClientRegistryUpdateIntervalInSecs() != other.getClientRegistryUpdateIntervalInSecs()) {
            return false;
        }
        if (getClientZoneId() != other.getClientZoneId()) {
            return false;
        }
        if (isDefaultClientEnabled() != other.isDefaultClientEnabled()) {
            return false;
        }
        if (getFailureDetectorAsyncRecoveryInterval() != other.getFailureDetectorAsyncRecoveryInterval()) {
            return false;
        }
        if (getFailureDetectorBannagePeriod() != other.getFailureDetectorBannagePeriod()) {
            return false;
        }
        if (!getFailureDetectorCatastrophicErrorTypes().equals(other.getFailureDetectorCatastrophicErrorTypes())) {
            return false;
        }
        if (!getFailureDetectorImplementation().equals(other.getFailureDetectorImplementation())) {
            return false;
        }
        if (getFailureDetectorRequestLengthThreshold() != other.getFailureDetectorRequestLengthThreshold()) {
            return false;
        }
        if (getFailureDetectorThreshold() != other.getFailureDetectorThreshold()) {
            return false;
        }
        if (getFailureDetectorThresholdCountMinimum() != other.getFailureDetectorThresholdCountMinimum()) {
            return false;
        }
        if (getFailureDetectorThresholdInterval() != other.getFailureDetectorThresholdInterval()) {
            return false;
        }
        if (isJmxEnabled() != other.isJmxEnabled()) {
            return false;
        }
        if (isLazyEnabled() != other.isLazyEnabled()) {
            return false;
        }
        if (getMaxBootstrapRetries() != other.getMaxBootstrapRetries()) {
            return false;
        }
        if (getMaxConnectionsPerNode() != other.getMaxConnectionsPerNode()) {
            return false;
        }
        if (getMaxQueuedRequests() != other.getMaxQueuedRequests()) {
            return false;
        }
        if (getMaxThreads() != other.getMaxThreads()) {
            return false;
        }
        if (getMaxTotalConnections() != other.getMaxTotalConnections()) {
            return false;
        }
        if (isPipelineRoutedStoreEnabled() != other.isPipelineRoutedStoreEnabled()) {
            return false;
        }
        if (!getRequestFormatType().equals(other.getRequestFormatType())) {
            return false;
        }
        if (!getRoutingTier().equals(other.getRoutingTier())) {
            return false;
        }
        if (getSelectors() != other.getSelectors()) {
            return false;
        }
        if (!getSerializerFactory().equals(other.getSerializerFactory())) {
            return false;
        }
        if (getSocketBufferSize() != other.getSocketBufferSize()) {
            return false;
        }
        if (getSocketKeepAlive() != other.getSocketKeepAlive()) {
            return false;
        }
        if (getSysConnectionTimeout() != other.getSysConnectionTimeout()) {
            return false;
        }
        if (getSysEnableJmx() != other.getSysEnableJmx()) {
            return false;
        }
        if (getSysEnablePipelineRoutedStore() != other.getSysEnablePipelineRoutedStore()) {
            return false;
        }
        if (getSysMaxConnectionsPerNode() != other.getSysMaxConnectionsPerNode()) {
            return false;
        }
        if (getSysRoutingTimeout() != other.getSysRoutingTimeout()) {
            return false;
        }
        if (getSysSocketTimeout() != other.getSysSocketTimeout()) {
            return false;
        }
        if (!getTimeoutConfig().equals(other.getTimeoutConfig())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getAsyncJobThreadPoolSize();
        result = prime * result + (int) getAsyncMetadataRefreshInMs();
        result = prime * result + Arrays.hashCode(getBootstrapUrls());
        result = prime * result + getClientContextName().hashCode();
        result = prime * result + getClientRegistryUpdateIntervalInSecs();
        result = prime * result + getClientZoneId();
        result = prime * result + (isDefaultClientEnabled() ? 1231 : 1237);
        result = prime * result + (int) getFailureDetectorAsyncRecoveryInterval();
        result = prime * result + (int) getFailureDetectorBannagePeriod();
        result = prime * result + getFailureDetectorCatastrophicErrorTypes().hashCode();
        result = prime * result + getFailureDetectorImplementation().hashCode();
        result = prime * result + (int) getFailureDetectorRequestLengthThreshold();
        result = prime * result + getFailureDetectorThreshold();
        result = prime * result + getFailureDetectorThresholdCountMinimum();
        result = prime * result + (int) getFailureDetectorThresholdInterval();
        result = prime * result + (isJmxEnabled() ? 1231 : 1237);
        result = prime * result + (isLazyEnabled() ? 1231 : 1237);
        result = prime * result + getMaxBootstrapRetries();
        result = prime * result + getMaxConnectionsPerNode();
        result = prime * result + getMaxQueuedRequests();
        result = prime * result + getMaxThreads();
        @SuppressWarnings("deprecation")
        final int connections = getMaxTotalConnections();
        result = prime * result + connections;
        result = prime * result + (isPipelineRoutedStoreEnabled() ? 1231 : 1237);
        result = prime * result + getRequestFormatType().hashCode();
        result = prime * result + getRoutingTier().hashCode();
        result = prime * result + getSelectors();
        result = prime * result + getSerializerFactory().hashCode();
        result = prime * result + getSocketBufferSize();
        result = prime * result + (getSocketKeepAlive() ? 1231 : 1237);
        result = prime * result + getSysConnectionTimeout();
        result = prime * result + (getSysEnableJmx() ? 1231 : 1237);
        result = prime * result + (getSysEnablePipelineRoutedStore() ? 1231 : 1237);
        result = prime * result + getSysMaxConnectionsPerNode();
        result = prime * result + getSysRoutingTimeout();
        result = prime * result + getSysSocketTimeout();
        result = prime * result + getTimeoutConfig().hashCode();
        return result;
    }

    /*package*/ SmartClientConfig() {
        super();
    }

    @SuppressWarnings("deprecation")
    /*package*/ SmartClientConfig(final ClientConfig source) {
        super();

        setAsyncJobThreadPoolSize(source.getAsyncJobThreadPoolSize());
        setAsyncMetadataRefreshInMs(source.getAsyncMetadataRefreshInMs());
        setBootstrapUrls(source.getBootstrapUrls());
        setClientContextName(source.getClientContextName());
        setClientRegistryUpdateIntervalInSecs(source.getClientRegistryUpdateIntervalInSecs());
        setClientZoneId(source.getClientZoneId());
        setDefaultClientEnabled(source.isDefaultClientEnabled());
        setFailureDetectorAsyncRecoveryInterval(source.getFailureDetectorAsyncRecoveryInterval());
        setFailureDetectorBannagePeriod(source.getFailureDetectorBannagePeriod());
        setFailureDetectorCatastrophicErrorTypes(source.getFailureDetectorCatastrophicErrorTypes());
        setFailureDetectorImplementation(source.getFailureDetectorImplementation());
        setFailureDetectorRequestLengthThreshold(source.getFailureDetectorRequestLengthThreshold());
        setFailureDetectorThreshold(source.getFailureDetectorThreshold());
        setFailureDetectorThresholdCountMinimum(source.getFailureDetectorThresholdCountMinimum());
        setFailureDetectorThresholdInterval(source.getFailureDetectorThresholdInterval());
        setEnableJmx(source.isJmxEnabled());
        setEnableLazy(source.isLazyEnabled());
        setMaxBootstrapRetries(source.getMaxBootstrapRetries());
        setMaxConnectionsPerNode(source.getMaxConnectionsPerNode());
        setMaxQueuedRequests(source.getMaxQueuedRequests());
        setMaxThreads(source.getMaxThreads());
        setMaxTotalConnections(source.getMaxTotalConnections());
        setEnablePipelineRoutedStore(source.isPipelineRoutedStoreEnabled());
        setRequestFormatType(source.getRequestFormatType());
        setRoutingTier(source.getRoutingTier());
        setSelectors(source.getSelectors());
        setSerializerFactory(source.getSerializerFactory());
        setSocketBufferSize(source.getSocketBufferSize());
        setSocketKeepAlive(source.getSocketKeepAlive());
        setSysEnableJmx(source.getSysEnableJmx());
        setSysEnablePipelineRoutedStore(source.getSysEnablePipelineRoutedStore());
        setTimeoutConfig(source.getTimeoutConfig());

        // FIXME: Can only be set through properties! Copy config to properties, create from there.
//      newConfig.setSysConnectionTimeout(config.getSysConnectionTimeout());
//      newConfig.setSysMaxConnectionsPerNode(config.getSysMaxConnectionsPerNode());
//      newConfig.setSysRoutingTimeout(config.getSysRoutingTimeout());
//      newConfig.setSysSocketTimeout(config.getSysSocketTimeout());
    }

    /*package*/ SmartClientConfig(final Properties properties) {
        super(properties);
    }

    /*package*/ SmartClientConfig(final File propertiesFile) {
        super(propertiesFile);
    }

    /**
     * Allow subclasses to implement deep copying in a different way.
     */
    /*package*/ SmartTimeoutConfig copyOfTimeoutConfig(final TimeoutConfig tConfig) {
        return SmartTimeoutConfig.copyOf(tConfig);
    }

    /**
     * Allow subclasses to implement deep copying in a different way.
     */
    /*package*/ List<String> copyOfBootstrapUrls(final List<String> bootstrapUrls) {
        return new ArrayList<>(bootstrapUrls);
    }

    /**
     * Allow subclasses to implement deep copying in a different way.
     */
    /*package*/ List<String> copyOfFailureDetectorCatastrophicErrorTypes(final List<String> errorTypes) {
        return new ArrayList<>(errorTypes);
    }

}
