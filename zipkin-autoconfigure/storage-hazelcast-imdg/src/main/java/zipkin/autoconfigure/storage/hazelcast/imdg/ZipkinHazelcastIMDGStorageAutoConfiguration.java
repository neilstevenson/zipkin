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
package zipkin.autoconfigure.storage.hazelcast.imdg;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;

import zipkin.storage.StorageComponent;
import zipkin.storage.hazelcast.imdg.HazelcastIMDGStorageBuilder;

/**
 * <P>Hazelcast IMDG configuration.
 * </P>
 * <P>We are using IMDG in client-server mode, meaning the storage
 * is external to this process. This process, the client has very
 * little configuration, not much more than connectivity information.
 * </P>
 */
@Configuration
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "hazelcast")
@EnableConfigurationProperties(ZipkinHazelcastIMDGStorageProperties.class)
public class ZipkinHazelcastIMDGStorageAutoConfiguration {

	@Autowired(required = false)
	private ZipkinHazelcastIMDGStorageProperties zipkinHazelcastIMDGStorageProperties;
	
	/**
	 * <P>Take the Hazelcast client configuration specified in
	 * {@code hazelcast-client.xml} and augment if properties
	 * are provided.
	 * </P>
	 * 
	 * @return Client config to use ot build a Hazelcast client
	 * @throws Exception If XML is corrupt
	 */
	@Bean
	public ClientConfig clientConfig() throws Exception {
		ClientConfig clientConfig = new XmlClientConfigBuilder("hazelcast-client.xml").build();

		if (this.zipkinHazelcastIMDGStorageProperties != null) {

			if (this.zipkinHazelcastIMDGStorageProperties.getGroupName()!=null) {
				clientConfig.getGroupConfig()
				.setName(this.zipkinHazelcastIMDGStorageProperties.getGroupName());
			}

			if (this.zipkinHazelcastIMDGStorageProperties.getGroupPassword()!=null) {
				clientConfig.getGroupConfig()
				.setPassword(this.zipkinHazelcastIMDGStorageProperties.getGroupPassword());
			}
				
			if (this.zipkinHazelcastIMDGStorageProperties.getClusterMemberCSV()!=null) {
				List<String> addresses 
					= Arrays.asList(this.zipkinHazelcastIMDGStorageProperties.getClusterMemberCSV().split(","));
				clientConfig.getNetworkConfig().setAddresses(addresses);
			}
		}

		return clientConfig;
	}
	
	/**
	 * <P>Default executor for handling asynchronous requests.
	 * </P>
	 * 
	 * @return 
	 */
	@Bean
	@ConditionalOnMissingBean(Executor.class)
	public Executor executor() {
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		
		threadPoolTaskExecutor.setThreadNamePrefix("ZipkinHazeclastIMDGStorage-");
		threadPoolTaskExecutor.initialize();
		
		return threadPoolTaskExecutor;
	}

	/**
	 * <P>Build trace storage using Hazelcast IMDG in client-server
	 * mode. This process is the client.
	 * </P>
	 * 
	 * @param strictTraceId 128 or 64 bit.
	 * @param clientConfig To connect to IMDG servers
	 * @param executor Executor for async requests 
	 * @return Storage
	 */
	@Bean
	@ConditionalOnMissingBean(StorageComponent.class)
	public StorageComponent storageComponent(
			@Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
			ClientConfig clientConfig,
			Executor executor) {
		
		return new HazelcastIMDGStorageBuilder()
				.clientConfig(clientConfig)
				.executor(executor)
				.strictTraceId(strictTraceId)
				.build();
	}

}
