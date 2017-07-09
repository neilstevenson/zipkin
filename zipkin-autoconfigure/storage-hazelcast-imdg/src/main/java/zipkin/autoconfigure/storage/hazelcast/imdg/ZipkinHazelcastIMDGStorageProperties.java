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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * <P>Optional properties to override the {@code hazelcast-client.xml}
 * file when building the connection to the Hazelcast IMDG cluster.
 * </P>
 */
@ConfigurationProperties("zipkin.storage.hazelcast")
public class ZipkinHazelcastIMDGStorageProperties {
	
	/**
	 * <P>Cluster connection credentials.
	 * </P>
	 */
	private String groupName;
	private String groupPassword;
	
	/**
	 * <P>Comma separated list of some of the cluster member processes.
	 * You don't need to specify them all, just one would be enough
	 * so long as it's up. The first member you find tells you the
	 * location of all the others.
	 * </P>
	 * <P>Eg. {@code alpha:5701,beta:5701,gamma:5701}
	 * </P>
	 */
	private String clusterMemberCSV;

	// Getters/Setters

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getGroupPassword() {
		return groupPassword;
	}

	public void setGroupPassword(String groupPassword) {
		this.groupPassword = groupPassword;
	}

	public String getClusterMemberCSV() {
		return clusterMemberCSV;
	}

	public void setClusterMemberCSV(String clusterMemberCSV) {
		this.clusterMemberCSV = clusterMemberCSV;
	}

}
