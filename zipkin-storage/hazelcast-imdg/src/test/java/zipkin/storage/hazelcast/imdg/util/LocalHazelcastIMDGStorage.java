/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.hazelcast.imdg.util;

import java.lang.reflect.Field;
import java.util.concurrent.Executors;

import com.hazelcast.core.HazelcastInstance;

import sun.misc.Unsafe;
import zipkin.storage.hazelcast.imdg.HazelcastIMDGSpanConsumer;
import zipkin.storage.hazelcast.imdg.HazelcastIMDGSpanStore;
import zipkin.storage.hazelcast.imdg.HazelcastIMDGStorage;

/**
 * <P><B>Test helper</B></P>
 * <P>{@link zipkin.storage.hazelcast.imdg.HazelcastIMDGStorage HazelcastIMDGStorage} uses
 * a client connection to a Hazelcast cluster that is external to this process. While
 * architecturally correct this makes it difficult for unit tests -- we would need to set
 * up such a cluster in this process and network in and out. This is flaky, for example
 * if the port is in use.
 * </P>
 * <P>Instead use reflection to make the {@code hazelcastInstance} field inside the
 * storage container a standalone Hazelcast server instead of a client.
 * </P>
 */
@SuppressWarnings("restriction")
public class LocalHazelcastIMDGStorage {

	private static Field executorField;
	private static Field hazelcastInstanceField;
	private static Field spanConsumerForAsyncField;
	private static Field spanStoreForAsyncField;
	private static Field spanStoreForSyncField;
	private static Field strictTraceIdField;
	private static Unsafe unsafe;
	private static HazelcastIMDGStorage hazelcastIMDGStorage;

	public static HazelcastIMDGStorage newHazelcastIMDGStorage(HazelcastInstance hazelcastInstance,
			boolean strictTraceIdOrNot) throws Exception {

		// Access static singleton
		Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
		unsafeField.setAccessible(true);
		unsafe = (Unsafe) unsafeField.get(null);

		executorField = HazelcastIMDGStorage.class.getDeclaredField("executor");
		executorField.setAccessible(true);
		
		hazelcastInstanceField = HazelcastIMDGStorage.class.getDeclaredField("hazelcastInstance");
		hazelcastInstanceField.setAccessible(true);

		spanConsumerForAsyncField = HazelcastIMDGStorage.class.getDeclaredField("spanConsumerForAsync");
		spanConsumerForAsyncField.setAccessible(true);

		spanStoreForAsyncField = HazelcastIMDGStorage.class.getDeclaredField("spanStoreForAsync");
		spanStoreForAsyncField.setAccessible(true);

		spanStoreForSyncField = HazelcastIMDGStorage.class.getDeclaredField("spanStoreForSync");
		spanStoreForSyncField.setAccessible(true);

		strictTraceIdField = HazelcastIMDGStorage.class.getDeclaredField("strictTraceId");
		strictTraceIdField.setAccessible(true);
		
		hazelcastIMDGStorage = (HazelcastIMDGStorage) unsafe.allocateInstance(HazelcastIMDGStorage.class);

		executorField.set(hazelcastIMDGStorage, Executors.newSingleThreadExecutor());
		
		hazelcastInstanceField.set(hazelcastIMDGStorage, hazelcastInstance);
		
		spanConsumerForAsyncField.set(hazelcastIMDGStorage,
				new HazelcastIMDGSpanConsumer(hazelcastInstance));
				
		spanStoreForAsyncField.set(hazelcastIMDGStorage, 
				new HazelcastIMDGSpanStore(strictTraceIdOrNot, hazelcastInstance));
				
		spanStoreForSyncField.set(hazelcastIMDGStorage, 
				new HazelcastIMDGSpanStore(strictTraceIdOrNot, hazelcastInstance));
		
		// Hazelcast supports 128-bit traces
		strictTraceIdField.set(hazelcastIMDGStorage, strictTraceIdOrNot);

		return hazelcastIMDGStorage;
	}

}
