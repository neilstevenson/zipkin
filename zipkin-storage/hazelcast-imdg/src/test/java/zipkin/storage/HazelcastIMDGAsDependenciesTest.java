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
package zipkin.storage;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import zipkin.storage.StorageComponent;
import zipkin.storage.hazelcast.imdg.HazelcastIMDGStorage;
import zipkin.storage.hazelcast.imdg.util.LocalHazelcastIMDGStorage;

/**
 * <P>Run generic tests, {@link DependenciesTest}.
 * </P>
 * <P>Use Hazelcast as implementation provider.
 * </P>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes={Object.class})
public class HazelcastIMDGAsDependenciesTest extends DependenciesTest {
	private static final boolean DEFAULT_STRICT_TRACE_ID = true;

	private static HazelcastInstance hazelcastInstance;
	
	private static HazelcastIMDGStorage hazelcastIMDGStorage;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		// Ensure no remnants from other tests
		Hazelcast.shutdownAll();

		Config config = new Config();
		config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		
		hazelcastIMDGStorage = 
			LocalHazelcastIMDGStorage.newHazelcastIMDGStorage(hazelcastInstance, DEFAULT_STRICT_TRACE_ID);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		hazelcastInstance.shutdown();
	}

	@Override
	protected StorageComponent storage() {
		return hazelcastIMDGStorage;
	}

	// Run @Before each @Test
	@Override
	public void clear() throws IOException {
		
		hazelcastInstance.getDistributedObjects()
			.stream()
				.filter(IMap.class::isInstance)
				.map(IMap.class::cast)
				.forEach(imap -> imap.clear());
		hazelcastInstance.getDistributedObjects()
			.stream()
				.filter(MultiMap.class::isInstance)
				.map(MultiMap.class::cast)
				.forEach(multiMap -> multiMap.clear());
	}

}
