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
package rickbw.crud.voldemort;

import rickbw.crud.ReadableResourceProvider;
import rickbw.crud.WritableResourceProvider;
import rickbw.crud.util.Preconditions;

import voldemort.client.StoreClient;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;


public class VoldemortResourceProvider<K, V>
implements ReadableResourceProvider<K, Versioned<V>>,
           WritableResourceProvider<K, Versioned<V>, Version> {

    private final StoreClient<K, V> store;


    public VoldemortResourceProvider(final StoreClient<K, V> store) {
        this.store = Preconditions.checkNotNull(store);
    }

    @Override
    public VoldemortResource<V> get(final K key) {
        return VoldemortResource.create(this.store, key);
    }

}
