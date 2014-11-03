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

import crud.pattern.ResourceMerger;
import crud.spi.DeletableResource;
import crud.spi.ReadableResource;
import crud.spi.WritableResource;
import rx.Observable;
import rx.Subscriber;

import voldemort.client.StoreClient;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;


public class VoldemortResource<T>
implements ReadableResource<Versioned<T>>,
           WritableResource<Versioned<T>, Version>,
           DeletableResource<Boolean> {

    private final KeyedStoreClient<?, T> store;
    private transient ResourceMerger<Version> merger = null;


    public ResourceMerger<Version> merger() {
        if (this.merger == null) {
            this.merger = ResourceMerger.withWriter(this, this);
        }
        return this.merger;
    }

    @Override
    public Observable<Versioned<T>> get() {
        final Observable<Versioned<T>> result = Observable.create(new Observable.OnSubscribe<Versioned<T>>() {
            @Override
            public void call(final Subscriber<? super Versioned<T>> subscriber) {
                try {
                    final Versioned<T> value = store.get();
                    subscriber.onNext(value);
                    subscriber.onCompleted();
                } catch (final Throwable error) {
                    subscriber.onError(error);
                }
            }
        });
        // TODO: Subscribe asynchronously?
        return result;
    }

    @Override
    public Observable<Version> write(final Versioned<T> newValue) {
        final Observable<Version> result = Observable.create(new Observable.OnSubscribe<Version>() {
            @Override
            public void call(final Subscriber<? super Version> subscriber) {
                try {
                    /* TODO: Provide variant that forces in-order writes by
                     * reading current value and writing another.
                     */
                    final Version version = store.put(newValue);
                    subscriber.onNext(version);
                    subscriber.onCompleted();
                } catch (final Throwable error) {
                    subscriber.onError(error);
                }
            }
        });
        // TODO: Subscribe asynchronously?
        return result;
    }

    @Override
    public Observable<Boolean> delete() {
        final Observable<Boolean> result = Observable.create(new Observable.OnSubscribe<Boolean>() {
            @Override
            public void call(final Subscriber<? super Boolean> subscriber) {
                try {
                    // XXX: Would be nice to be able to delete a particular version.
                    final Boolean deleted = store.delete();
                    subscriber.onNext(deleted);
                    subscriber.onCompleted();
                } catch (final Throwable error) {
                    subscriber.onError(error);
                }
            }
        });
        // TODO: Subscribe asynchronously?
        return result;
    }

    /*package*/ static <K, V> VoldemortResource<V> create(final StoreClient<K, V> store, final K key) {
        return new VoldemortResource<>(new KeyedStoreClient<K, V>(store, key));
    }

    private VoldemortResource(final KeyedStoreClient<?, T> store) {
        this.store = store;
        assert this.store != null;
    }

}
