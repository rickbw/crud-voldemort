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
package crud.voldemort;

import java.util.Objects;

import crud.rsrc.Deletable;
import crud.rsrc.Readable;
import crud.rsrc.Writable;
import crud.spi.DeletableProviderSpec;
import crud.spi.ReadableProviderSpec;
import crud.spi.WritableProviderSpec;

import voldemort.client.StoreClient;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;


public class VoldemortResourceProvider<K, V>
implements ReadableProviderSpec<K, Versioned<V>>,
           WritableProviderSpec<K, Versioned<V>, Version>,
           DeletableProviderSpec<K, Boolean>{

    private final StoreClient<K, V> store;


    public VoldemortResourceProvider(final StoreClient<K, V> store) {
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public Readable<Versioned<V>> reader(final K key) {
        return Readable.from(VoldemortResource.create(this.store, key));
    }

    @Override
    public Writable<Versioned<V>, Version> writer(final K key) {
        return Writable.from(VoldemortResource.create(this.store, key));
    }

    @Override
    public Deletable<Boolean> deleter(final K key) {
        return Deletable.from(VoldemortResource.create(this.store, key));
    }

}
