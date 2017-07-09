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
package zipkin.storage.hazelcast.imdg;

import static zipkin.storage.StorageAdapters.blockingToAsync;

import java.io.IOException;
import java.util.concurrent.Executor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;

import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.AsyncSpanStore;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageAdapters.SpanConsumer;
import zipkin.storage.StorageComponent;

/**
 * <P>Hazelcast IMDG storage delegate. Use
 * {@link com.hazelcast.client.HazelcastClient HazelcastClient} to
 * create a connection to the IMDG store.
 * </P>
 * <P>Create two instances of the span store, for sync and async
 * calls, in case useful for metrics of which is used most.
 * </P>
 * <P>Here {@code spanConsumerForAsync} and {@code spanStoreForAsync}
 * share an {@code executor}. Could be changed to give them a pool
 * of their own each.
 * </P>
 */
public class HazelcastIMDGStorage implements StorageComponent {

	private final boolean strictTraceId;
	private final Executor executor;
	private final HazelcastInstance hazelcastInstance;
	private final SpanConsumer spanConsumerForAsync;
	private final SpanStore spanStoreForAsync;
	private final SpanStore spanStoreForSync;
	
	public HazelcastIMDGStorage(ClientConfig clientConfig, boolean arg1, Executor arg2) {		
		this.hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
		this.strictTraceId = arg1;
		this.executor = arg2;
		
		this.spanConsumerForAsync = new HazelcastIMDGSpanConsumer(this.hazelcastInstance);
		this.spanStoreForAsync = new HazelcastIMDGSpanStore(this.strictTraceId, this.hazelcastInstance);
		this.spanStoreForSync = new HazelcastIMDGSpanStore(this.strictTraceId, this.hazelcastInstance);
	}

	/**
	 * <P>Not running after close.
	 * </P>
	 */
	@Override
	public CheckResult check() {
		try {
			LifecycleService lifecycleService = this.hazelcastInstance.getLifecycleService();
			
			if (!lifecycleService.isRunning()) {
				throw new RuntimeException("hazelcastInstance.getLifecycleService().isRunning()==false");
			}
			
		} catch (Exception exception) {
			return CheckResult.failed(exception);
		}
		return CheckResult.OK;
	}

	@Override
	public void close() throws IOException {
		this.hazelcastInstance.shutdown();
	}

	@Override
	public SpanStore spanStore() {
		return this.spanStoreForSync;
	}

	@Override
	public AsyncSpanStore asyncSpanStore() {
		return blockingToAsync(this.spanStoreForAsync, this.executor);
	}

	@Override
	public AsyncSpanConsumer asyncSpanConsumer() {
		return blockingToAsync(this.spanConsumerForAsync, this.executor);
	}

	public boolean isStrictTraceId() {
		return strictTraceId;
	}
}
