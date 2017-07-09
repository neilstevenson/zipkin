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
package zipkin.autoconfig.storage.hazelcast.imdg;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.client.config.ClientConfig;

import zipkin.autoconfigure.storage.hazelcast.imdg.ZipkinHazelcastIMDGStorageAutoConfiguration;
import zipkin.storage.StorageComponent;
import zipkin.storage.hazelcast.imdg.HazelcastIMDGStorage;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes={Object.class})
public class ZipkinHazelcastIMDGStorageAutoConfigurationTest {
	
	private AnnotationConfigApplicationContext annotationConfigApplicationContext;

	// Real instance makes eager connection to IMDG cluster
	@MockBean
	private HazelcastIMDGStorage mockHazelcastIMDGStorage;
	
	@Before
	public void setUp() throws Exception {
		this.annotationConfigApplicationContext = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(this.annotationConfigApplicationContext, 
				"zipkin.storage.type:hazelcast");
	}

	@After
	public void tearDown() throws Exception {
		if (this.annotationConfigApplicationContext!=null) {
			this.annotationConfigApplicationContext.close();
		}
	}
	
	@Test(expected=NoSuchBeanDefinitionException.class)
	public void onlyProvidesStorageForHazelcast() {
		EnvironmentTestUtils.addEnvironment(this.annotationConfigApplicationContext, 
				"zipkin.storage.type:something-else");

	    this.annotationConfigApplicationContext.register(
	    		PropertyPlaceholderAutoConfiguration.class,
	    		ZipkinHazelcastIMDGStorageAutoConfiguration.class);
	    
	    this.annotationConfigApplicationContext.refresh();
	    
	    this.annotationConfigApplicationContext.getBean(StorageComponent.class);
	}

	@Test(expected=NoSuchBeanDefinitionException.class)
	public void onlyClientConfigIfHazelcast() {
		EnvironmentTestUtils.addEnvironment(this.annotationConfigApplicationContext, 
				"zipkin.storage.type:something-else");

	    this.annotationConfigApplicationContext.register(
	    		PropertyPlaceholderAutoConfiguration.class,
	    		ZipkinHazelcastIMDGStorageAutoConfiguration.class);
	    
	    this.annotationConfigApplicationContext.refresh();
	    
	    this.annotationConfigApplicationContext.getBean(ClientConfig.class);
	}

	@Test
	public void doesProvidesStorageForHazelcast() {
	    this.annotationConfigApplicationContext.register(
	    		PropertyPlaceholderAutoConfiguration.class,
	    		ZipkinHazelcastIMDGStorageAutoConfiguration.class
	    		);

	    this.annotationConfigApplicationContext
	    	.getBeanFactory()
	    	.registerSingleton("storageComponent", this.mockHazelcastIMDGStorage);
	    
	    this.annotationConfigApplicationContext.refresh();
	    
	    this.annotationConfigApplicationContext.getBean(HazelcastIMDGStorage.class);
	}

	@Test
	public void defaultProperties() {
	    this.annotationConfigApplicationContext.register(
	    		PropertyPlaceholderAutoConfiguration.class,
	    		ZipkinHazelcastIMDGStorageAutoConfiguration.class);

	    this.annotationConfigApplicationContext
	    	.getBeanFactory()
	    	.registerSingleton("storageComponent", this.mockHazelcastIMDGStorage);

	    this.annotationConfigApplicationContext.refresh();

	    ClientConfig clientConfig
	    	= this.annotationConfigApplicationContext.getBean(ClientConfig.class);
	    
	    assertThat("Group name", clientConfig.getGroupConfig().getName(),
	    		equalTo("dev"));
	    assertThat("Group pass", clientConfig.getGroupConfig().getPassword(),
	    		equalTo("dev-pass"));
	    assertThat("Group members", clientConfig.getNetworkConfig().getAddresses(),
	    		hasItem("127.0.0.1:5701"));
	    assertThat("Group members", clientConfig.getNetworkConfig().getAddresses(),
	    		IsCollectionWithSize.hasSize(1));
	    assertThat("Group members", clientConfig.getNetworkConfig().getAddresses(),
	    		hasItem("127.0.0.1:5701"));

	    /* As storage is mocked, testing other param sessions is pointless. 
	     */
	    //HazelcastIMDGStorage hazelcastIMDGStorage
    	//	= this.annotationConfigApplicationContext.getBean(HazelcastIMDGStorage.class);
	    //org.mockito.Mockito.when(this.mockHazelcastIMDGStorage.isStrictTraceId()).thenReturn(true);
	    //assertThat("strict-trace-id", hazelcastIMDGStorage.isStrictTraceId(),
	    //		is(true));
	}
	
	@Test
	public void overrideGroupName() {
		EnvironmentTestUtils.addEnvironment(this.annotationConfigApplicationContext, 
				"zipkin.storage.hazelcast.groupName:prod");
		
	    this.annotationConfigApplicationContext.register(
	    		PropertyPlaceholderAutoConfiguration.class,
	    		ZipkinHazelcastIMDGStorageAutoConfiguration.class);

	    this.annotationConfigApplicationContext
	    	.getBeanFactory()
	    	.registerSingleton("storageComponent", this.mockHazelcastIMDGStorage);

	    this.annotationConfigApplicationContext.refresh();

	    ClientConfig clientConfig
    	= this.annotationConfigApplicationContext.getBean(ClientConfig.class);
    
	    assertThat("Group name", clientConfig.getGroupConfig().getName(),
    		not(equalTo("dev")));
	    assertThat("Group pass", clientConfig.getGroupConfig().getPassword(),
	    	equalTo("dev-pass"));
	}

	@Test
	public void overrideGroupPassword() {
		EnvironmentTestUtils.addEnvironment(this.annotationConfigApplicationContext, 
				"zipkin.storage.hazelcast.groupPassword:password");
		
	    this.annotationConfigApplicationContext.register(
	    		PropertyPlaceholderAutoConfiguration.class,
	    		ZipkinHazelcastIMDGStorageAutoConfiguration.class);

	    this.annotationConfigApplicationContext
	    	.getBeanFactory()
	    	.registerSingleton("storageComponent", this.mockHazelcastIMDGStorage);

	    this.annotationConfigApplicationContext.refresh();

	    ClientConfig clientConfig
    	= this.annotationConfigApplicationContext.getBean(ClientConfig.class);
    
	    assertThat("Group name", clientConfig.getGroupConfig().getName(),
    		equalTo("dev"));
	    assertThat("Group pass", clientConfig.getGroupConfig().getPassword(),
	    	not(equalTo("dev-pass")));
	}

	@Test
	public void overrideClusterMemberCSV() {
		String THREE_CSV_IPS = "1.2.3.4:5701,3.4.5.6:5701,5.6.7.8:5701";
		
		EnvironmentTestUtils.addEnvironment(this.annotationConfigApplicationContext, 
				"zipkin.storage.hazelcast.clusterMemberCSV:" + THREE_CSV_IPS);

		this.annotationConfigApplicationContext.register(
	    		PropertyPlaceholderAutoConfiguration.class,
	    		ZipkinHazelcastIMDGStorageAutoConfiguration.class);

	    this.annotationConfigApplicationContext
	    	.getBeanFactory()
	    	.registerSingleton("storageComponent", this.mockHazelcastIMDGStorage);

	    this.annotationConfigApplicationContext.refresh();

	    ClientConfig clientConfig
    	= this.annotationConfigApplicationContext.getBean(ClientConfig.class);

	    assertThat("Group members", clientConfig.getNetworkConfig().getAddresses(),
	    		IsCollectionWithSize.hasSize(3));
	    assertThat("Not localhost", clientConfig.getNetworkConfig().getAddresses(),
	    		not(hasItem("127.0.0.1:5701")));
	    assertThat("Remote hosts", clientConfig.getNetworkConfig().getAddresses(),
	    		hasItems("1.2.3.4:5701","3.4.5.6:5701", "5.6.7.8:5701"));

	}

}
