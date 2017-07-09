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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import sun.misc.Unsafe;
import zipkin.Component.CheckResult;

/**
 * <P>Test the lifecycle methods on the storage. Use a Hazelcast server without
 * networking rather than a client so can test in a unit test.
 * </P>
 */
@SuppressWarnings("restriction")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes={Object.class})
public class HazelcastIMDGStorageTest {

	private static final boolean DEFAULT_STRICT_TRACE_ID = true;

	private static Config config;
	private static Field hazelcastInstanceField;
	private static Field executorField;
	private static Field strictTraceIdField;
	private static Unsafe unsafe;
	
	private HazelcastInstance hazelcastInstance;
	private HazelcastIMDGStorage hazelcastIMDGStorage;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		// Ensure no remnants from other tests
		Hazelcast.shutdownAll();

		config = new Config();
		config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
		
		hazelcastInstanceField = HazelcastIMDGStorage.class.getDeclaredField("hazelcastInstance");
		hazelcastInstanceField.setAccessible(true);
		
		executorField = HazelcastIMDGStorage.class.getDeclaredField("executor");
		executorField.setAccessible(true);
		
		strictTraceIdField = HazelcastIMDGStorage.class.getDeclaredField("strictTraceId");
		strictTraceIdField.setAccessible(true);
		
		// Access static singleton
		Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
		unsafeField.setAccessible(true);
		unsafe = (Unsafe) unsafeField.get(null);
	}

	@Before
	public void setUp() throws Exception {
		this.hazelcastIMDGStorage = (HazelcastIMDGStorage) unsafe.allocateInstance(HazelcastIMDGStorage.class);

		this.hazelcastInstance = Hazelcast.newHazelcastInstance(config);

		executorField.set(hazelcastIMDGStorage, Executors.newSingleThreadExecutor());
		
		hazelcastInstanceField.set(this.hazelcastIMDGStorage, this.hazelcastInstance);
		
		strictTraceIdField.set(this.hazelcastIMDGStorage, !DEFAULT_STRICT_TRACE_ID);
	}

	@After
	public void tearDown() throws Exception {
		Hazelcast.shutdownAll();
	}
	
	@Test
	public void checkOnce() {
		CheckResult checkResult = this.hazelcastIMDGStorage.check();
		assertThat(checkResult, is(equalTo(CheckResult.OK)));
	}
	
	@Test
	public void checkTwice() throws Exception {
		CheckResult checkResult1 = this.hazelcastIMDGStorage.check();
		assertThat("CheckResult before", checkResult1, is(equalTo(CheckResult.OK)));
		this.hazelcastIMDGStorage.close();
		CheckResult checkResult2 = this.hazelcastIMDGStorage.check();
		assertThat("CheckResult after", checkResult2, is(not(equalTo(CheckResult.OK))));
		assertThat("CheckResult after", checkResult2.exception.getMessage(), startsWith("hazelcast"));
	}
	
	@Test
	public void closeOnce() throws Exception {
		assertThat("Count before", Hazelcast.getAllHazelcastInstances().size(), is(equalTo(1)));
		this.hazelcastIMDGStorage.close();
		assertThat("Count after", Hazelcast.getAllHazelcastInstances().size(), is(equalTo(0)));
	}
	
	@Test
	public void closeTwice() throws Exception {
		assertThat("Count before", Hazelcast.getAllHazelcastInstances().size(), is(equalTo(1)));
		this.hazelcastIMDGStorage.close();
		assertThat("Count during", Hazelcast.getAllHazelcastInstances().size(), is(equalTo(0)));
		this.hazelcastIMDGStorage.close();
		assertThat("Count after", Hazelcast.getAllHazelcastInstances().size(), is(equalTo(0)));
	}
	
	@Test
	public void strictTraceId() {
		assertThat(this.hazelcastIMDGStorage.isStrictTraceId(), is(not(equalTo(DEFAULT_STRICT_TRACE_ID))));
	}

}
