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

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import voldemort.client.TimeoutConfig;


/**
 * A subclass of {@link TimeoutConfig} that provides deep copying,
 * stringification, equality, and proper behavior in hashing-based
 * {@link Set}s and {@link Map}s. It can otherwise be used interchangeably
 * with its superclass.
 */
public class SmartTimeoutConfig extends TimeoutConfig {

    /**
     * @see TimeoutConfig#TimeoutConfig(long, boolean)
     */
    public static SmartTimeoutConfig create(final long globalTimeout, final boolean allowPartialGetAlls) {
        return new SmartTimeoutConfig(globalTimeout, allowPartialGetAlls);
    }

    /**
     * @see TimeoutConfig#TimeoutConfig(long, long, long, long, long, boolean)
     */
    public static SmartTimeoutConfig create(
            final long getTimeout,
            final long putTimeout,
            final long deleteTimeout,
            final long getAllTimeout,
            final long getVersionsTimeout,
            final boolean allowPartialGetAlls) {
        return new SmartTimeoutConfig(
                getTimeout,
                putTimeout,
                deleteTimeout,
                getAllTimeout,
                getVersionsTimeout,
                allowPartialGetAlls);
    }

    /**
     * Create a new {@link TimeoutConfig} as a deep copy of another. All op
     * codes enumerated in {@link SmartOpCode} will be copied if present.
     */
    public static SmartTimeoutConfig copyOf(final TimeoutConfig source) {
        return new SmartTimeoutConfig(source);
    }

    /**
     * @throws NullPointerException if {@code opCode} is {@code null}.
     *
     * @see #setOperationTimeout(Byte, long)
     */
    public void setOperationTimeout(final SmartOpCode opCode, final long timeoutMs) {
        super.setOperationTimeout(opCode.byteValue(), timeoutMs);
    }

    /**
     * This implementation behaves the same as the inherited one, except that
     * it prevents setting {@code null} op codes.
     *
     * @throws NullPointerException if {@code opCode} is {@code null}.
     *
     * @see TimeoutConfig#setOperationTimeout(Byte, long)
     */
    @Override
    public void setOperationTimeout(final Byte opCode, final long timeoutMs) {
        Objects.requireNonNull(opCode, "null op code");
        super.setOperationTimeout(opCode, timeoutMs);
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder(getClass().getSimpleName());
        buf.append(" [");
        buf.append("isPartialGetAllAllowed=").append(isPartialGetAllAllowed());
        for (final SmartOpCode op : SmartOpCode.values()) {
            buf.append(", ").append(op.name()).append('=').append(op.byteValue());
        }
        buf.append(']');
        return buf.toString();
    }

    /**
     * This {@link SmartTimeoutConfig} is considered equal to any other
     * {@code TimeoutConfig} that has the same value for
     * {@link #isPartialGetAllAllowed()} and for all op codes enumerated in
     * {@link SmartOpCode}. <em>Note</em> that concrete instances of
     * {@code SmartTimeoutConfig} and {@link ImmutableTimeoutConfig} will
     * therefore be considered transitively equal to one another under those
     * circumstances, regardless of the fact that one side may change such
     * that the instances are no longer equal in the future. Furthermore, any
     * instance of {@code SmartTimeoutConfig} will be considered equal to a
     * concrete instance of {@code TimeoutConfig} under those circumstances;
     * however, such equality will not be transitive, since
     * {@code TimeoutConfig} does not implement {@code equals} itself.
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof TimeoutConfig)) {
            return false;
        }
        final TimeoutConfig other = (TimeoutConfig) obj;
        if (isPartialGetAllAllowed() != other.isPartialGetAllAllowed()) {
            return false;
        }
        for (final SmartOpCode op : SmartOpCode.values()) {
            final long mine = getOperationTimeout(op.byteValue());
            final long theirs = other.getOperationTimeout(op.byteValue());
            if (mine != theirs) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isPartialGetAllAllowed() ? 1231 : 1237);
        for (final SmartOpCode op : SmartOpCode.values()) {
            final long timeout = getOperationTimeout(op.byteValue());
            result = prime * result + (int) (timeout ^ (timeout >>> 32));
        }
        return result;
    }

    /*package*/ SmartTimeoutConfig(final long globalTimeout, final boolean allowPartialGetAlls) {
        super(globalTimeout, allowPartialGetAlls);
    }

    /*package*/ SmartTimeoutConfig(
            final long getTimeout,
            final long putTimeout,
            final long deleteTimeout,
            final long getAllTimeout,
            final long getVersionsTimeout,
            final boolean allowPartialGetAlls) {
        super(getTimeout,
              putTimeout,
              deleteTimeout,
              getAllTimeout,
              getVersionsTimeout,
              allowPartialGetAlls);
    }

    /*package*/ SmartTimeoutConfig(final TimeoutConfig source) {
        super(0L,   // irrelevant: will overwrite
              source.isPartialGetAllAllowed());
        for (final SmartOpCode op : SmartOpCode.values()) {
            final long timeout = source.getOperationTimeout(op.byteValue());
            setOperationTimeout(op.byteValue(), timeout);
        }
    }

}
