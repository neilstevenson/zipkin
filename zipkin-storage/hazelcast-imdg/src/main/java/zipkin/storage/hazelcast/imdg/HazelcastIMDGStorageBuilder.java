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

import java.util.concurrent.Executor;

import com.hazelcast.client.config.ClientConfig;

import zipkin.storage.StorageComponent;
import zipkin.storage.StorageComponent.Builder;

/**
 * <P>Fluent style builder class for Hazelcast IMDG.
 * </P>
 */
public class HazelcastIMDGStorageBuilder implements Builder {

	private boolean strictTraceId = true;
	private ClientConfig clientConfig = null;
	private Executor executor = null;
	
	@Override
	public HazelcastIMDGStorageBuilder strictTraceId(boolean arg0) {
		this.strictTraceId = arg0;
		return this;
	}

	public HazelcastIMDGStorageBuilder clientConfig(ClientConfig arg0) {
		this.clientConfig = arg0;
		return this;
	}

	public HazelcastIMDGStorageBuilder executor(Executor arg0) {
		this.executor = arg0;
		return this;
	}
	
	/**
	 * <P>Build connection to Hazelcast IMDG storage.
	 * </P>
	 */
	@Override
	public StorageComponent build() {
		return new HazelcastIMDGStorage(this.clientConfig, this.strictTraceId, this.executor);
	}

}
