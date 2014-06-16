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

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import voldemort.client.TimeoutConfig;


/**
 * An implementation of {@link TimeoutConfig} that prevents mutation as well
 * as providing deep equality, copying, and proper behavior in hashing-based
 * {@link Map}s and {@link Set}s.
 */
public final class ImmutableTimeoutConfig extends SmartTimeoutConfig {

    /**
     * Prevent mutation after the initial deep copy performed in super's
     * constructor. It's actually read before it's initialized.
     */
    private boolean locked = false;


    public static Builder newBuilder(final long defaultTimeout) {
        return new Builder(defaultTimeout);
    }

    public static Builder newBuilderFrom(final TimeoutConfig source) {
        return new Builder(SmartTimeoutConfig.copyOf(source));
    }

    /**
     * Create a new {@link TimeoutConfig} as a deep copy of another. All op
     * codes enumerated in {@link SmartOpCode} will be copied if present.
     */
    public static ImmutableTimeoutConfig copyOf(final TimeoutConfig source) {
        if (source instanceof ImmutableTimeoutConfig) {
            return (ImmutableTimeoutConfig) source;
        } else {
            return new ImmutableTimeoutConfig(source);
        }
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Deprecated
    @Override
    public void setOperationTimeout(final Byte opCode, final long timeoutMs) {
        checkLocked();
        super.setOperationTimeout(opCode, timeoutMs);
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Deprecated
    @Override
    public void setPartialGetAllAllowed(final boolean allowPartialGetAlls) {
        checkLocked();
        super.setPartialGetAllAllowed(allowPartialGetAlls);
    }

    private ImmutableTimeoutConfig(final TimeoutConfig source) {
        super(source);
        this.locked = true;
    }

    private void checkLocked() {
        if (this.locked) {
            throw new UnsupportedOperationException();
        }
    }


    public static final class Builder {
        private final TimeoutConfig config;

        private Builder(final long defaultTimeout) {
            this.config = SmartTimeoutConfig.create(defaultTimeout, false);
        }

        private Builder(final TimeoutConfig source) {
            this.config = Objects.requireNonNull(source);
        }

        /**
         * @see SmartTimeoutConfig#setPartialGetAllAllowed(boolean)
         */
        public Builder setPartialGetAllAllowed(final boolean allow) {
            this.config.setPartialGetAllAllowed(allow);
            return this;
        }

        /**
         * @see SmartTimeoutConfig#setOperationTimeout(Byte, long)
         */
        public Builder setOperationTimeout(final Byte op, final long timeout) {
            this.config.setOperationTimeout(op, timeout);
            return this;
        }

        public Builder setOperationTimeout(final SmartOpCode op, final long timeout) {
            return setOperationTimeout(op.byteValue(), timeout);
        }

        public ImmutableTimeoutConfig build() {
            return new ImmutableTimeoutConfig(this.config);
        }
    }

}
