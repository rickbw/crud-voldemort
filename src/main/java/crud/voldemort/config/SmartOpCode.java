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

import voldemort.common.VoldemortOpCode;


/**
 * An alternative to {@link VoldemortOpCode} that provides iterability and
 * named values.
 */
public enum SmartOpCode {
    DELETE_OP_CODE(VoldemortOpCode.DELETE_OP_CODE),
    DELETE_PARTITIONS_OP_CODE(VoldemortOpCode.DELETE_PARTITIONS_OP_CODE),
    GET_ALL_OP_CODE(VoldemortOpCode.GET_ALL_OP_CODE),
    GET_METADATA_OP_CODE(VoldemortOpCode.GET_METADATA_OP_CODE),
    GET_OP_CODE(VoldemortOpCode.GET_OP_CODE),
    GET_PARTITION_AS_STREAM_OP_CODE(VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE),
    GET_VERSION_OP_CODE(VoldemortOpCode.GET_VERSION_OP_CODE),
    PUT_ENTRIES_AS_STREAM_OP_CODE(VoldemortOpCode.PUT_ENTRIES_AS_STREAM_OP_CODE),
    PUT_OP_CODE(VoldemortOpCode.PUT_OP_CODE),
    REDIRECT_GET_OP_CODE(VoldemortOpCode.REDIRECT_GET_OP_CODE),
    UPDATE_METADATA_OP_CODE(VoldemortOpCode.UPDATE_METADATA_OP_CODE),
    ;

    private final Byte byteValue;


    private SmartOpCode(final Byte byteValue) {
        this.byteValue = byteValue;
        assert this.byteValue != null;
    }

    public Byte byteValue() {
        /* This method returns Byte instead of byte because
         * TimeoutConfig.setOperationTimeout() accepts Byte, and we want to
         * minimize the number of unboxing and re-boxing operations.
         */
        return this.byteValue;
    }

}
