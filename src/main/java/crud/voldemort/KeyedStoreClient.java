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

import voldemort.client.StoreClient;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;


/**
 * A wrapper for {@link StoreClient} that builds in the key, so that
 * {@link VoldemortResource} only needs a single generic type parameter.
 */
/*package*/ class KeyedStoreClient<K, V> {

    private final StoreClient<K, V> store;
    private final K key;


    public KeyedStoreClient(final StoreClient<K, V> store, final K key) {
        this.store = store;
        this.key = key;
        assert this.store != null;
        assert this.key != null;
    }

    /**
     * @see StoreClient#getValue(Object)
     */
    public V getValue() {
        return this.store.getValue(this.key);
    }

    /**
     * @see StoreClient#getValue(Object, Object)
     */
    public V getValue(final V defaultValue) {
        return this.store.getValue(this.key, defaultValue);
    }

    /**
     * @see StoreClient#get(Object)
     */
    public Versioned<V> get() {
        return this.store.get(this.key);
    }

    /**
     * @see StoreClient#get(Object, Object)
     */
    public Versioned<V> get(final Object transforms) {
        return this.store.get(this.key, transforms);
    }

    /**
     * @see StoreClient#get(Object, Versioned)
     */
    public Versioned<V> get(final Versioned<V> defaultValue) {
        return this.store.get(this.key, defaultValue);
    }

    /**
     * @see StoreClient#put(Object, Object)
     */
    public Version put(final V value) {
        return this.store.put(this.key, value);
    }

    /**
     * @see StoreClient#put(Object, Object, Object)
     */
    public Version put(final V value, final Object transforms) {
        return this.store.put(this.key, value, transforms);
    }

    /**
     * @see StoreClient#put(Object, Versioned)
     */
    public Version put(final Versioned<V> versioned) throws ObsoleteVersionException {
        return this.store.put(this.key, versioned);
    }

    /**
     * @see StoreClient#putIfNotObsolete(Object, Versioned)
     */
    public boolean putIfNotObsolete(final Versioned<V> versioned) {
        return this.store.putIfNotObsolete(this.key, versioned);
    }

    /**
     * @see StoreClient#delete(Object)
     */
    public boolean delete() {
        return this.store.delete(this.key);
    }

    /**
     * @see StoreClient#delete(Object, Version)
     */
    public boolean delete(final Version version) {
        return this.store.delete(this.key, version);
    }

}
