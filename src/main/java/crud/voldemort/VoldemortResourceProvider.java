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
import crud.rsrc.Gettable;
import crud.rsrc.Settable;
import crud.spi.DeletableSetSpec;
import crud.spi.GettableSetSpec;
import crud.spi.SettableSetSpec;

import voldemort.client.StoreClient;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;


public class VoldemortResourceProvider<K, V>
implements GettableSetSpec<K, Versioned<V>>,
           SettableSetSpec<K, Versioned<V>, Version>,
           DeletableSetSpec<K, Boolean>{

    private final StoreClient<K, V> store;


    public VoldemortResourceProvider(final StoreClient<K, V> store) {
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public Gettable<Versioned<V>> getter(final K key) {
        return Gettable.from(VoldemortResource.create(this.store, key));
    }

    @Override
    public Settable<Versioned<V>, Version> setter(final K key) {
        return Settable.from(VoldemortResource.create(this.store, key));
    }

    @Override
    public Deletable<Boolean> deleter(final K key) {
        return Deletable.from(VoldemortResource.create(this.store, key));
    }

}
