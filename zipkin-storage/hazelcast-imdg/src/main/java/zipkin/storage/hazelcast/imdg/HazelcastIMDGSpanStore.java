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

import static zipkin.internal.GroupByTraceId.TRACE_DESCENDING;
import static zipkin.internal.Util.sortedList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import zipkin.Annotation;
import zipkin.DependencyLink;
import zipkin.Span;
import zipkin.internal.CorrectForClockSkew;
import zipkin.internal.DependencyLinker;
import zipkin.internal.GroupByTraceId;
import zipkin.internal.MergeById;
import zipkin.internal.Pair;
import zipkin.storage.QueryRequest;
import zipkin.storage.SpanStore;

/**
 * <P>Hazelcast storage is distributed, split across several machines,
 * so can only do any sorting locally.
 * </P>
 * <OL>
 * <LI>Service Names : Spans
 * <P>Service names are stored in a {@link com.hazelcast.core.MultiMap MultiMap}
 * where the key is the service name string and value a set of span names.
 * </P>
 * </LI>
 * <LI>Spans
 * <P>Spans are stored in a {@link com.hazelcast.core.IMap IMap}, which
 * more of less behaves like an {@link java.util.Map}, except we can
 * search on the map values.
 * </P>As a map is a {@code key-value} store, use the whole span as the
 * value, and duplicate some of the fields from the value to form the
 * key. This is slightly wasteful on space but simplifies the logic
 * and searching.
 * </P>
 * </LI>
 * </OL>
 */
public class HazelcastIMDGSpanStore implements SpanStore {

	protected static final boolean IS_SORTED = true;
	protected static final boolean IS_NOT_SORTED = false;
	
	private final boolean strictTraceId;
	private final HazelcastInstance hazelcastInstance;
	
	private final MultiMap<String, String> services;
	private final IMap<HazelcastIMDGSpanKey, Span> spans;

	public HazelcastIMDGSpanStore(boolean arg0, HazelcastInstance arg1) {
		this.strictTraceId = arg0;
		this.hazelcastInstance = arg1;

		this.services =
				this.hazelcastInstance.getMultiMap(HazelcastIMDGConstants.MULTIMAP_NAME__ZIPKIN_SERVICES);
		this.spans = 
				this.hazelcastInstance.getMap(HazelcastIMDGConstants.IMAP_NAME__ZIPKIN_SPANS);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<List<Span>> getTraces(QueryRequest request) {

		String effectiveServiceName = this._cleanseString(request.serviceName);
		String effectiveSpanName = this._cleanseString(request.spanName);

		// Build up a search predicate from provided query request
		Predicate<HazelcastIMDGSpanKey, Span> predicate = null;

		/* If service name provided search for span names. Then
		 * try span name.
		 */
		Collection<String> spanNames = new ArrayList<>();
		if (effectiveServiceName!=null && effectiveSpanName==null) {
			spanNames = services.get(effectiveServiceName);
		} else {
			if (effectiveSpanName!=null) {
				spanNames.add(effectiveSpanName);
			}
		}
		if (!spanNames.isEmpty()) {
			for (String spanName : spanNames) {
				if (predicate==null) {
					predicate = Predicates.equal("name", spanName);
				} else {
					predicate = Predicates.or(predicate, Predicates.equal("name", spanName));
				}
			}
		}
		
		// Find all trace ids for the spans by name or spans by service name
		if (predicate!=null) {
			Projection projection = Projections.multiAttribute("traceIdHigh", "traceId");
			Collection<Object[]> traces = spans.project(projection, predicate);
			predicate = null;
			if (traces!=null) {
				for (Object[] trace : traces) {
					if (predicate==null) {
						predicate = 
								Predicates.and(Predicates.equal("traceIdHigh", trace[0].toString())
											,Predicates.equal("traceId", trace[1].toString())
										)
								;
					} else {
						predicate = Predicates.or(predicate,
								Predicates.and(Predicates.equal("traceIdHigh", trace[0].toString())
											,Predicates.equal("traceId", trace[1].toString())
										)
								);
					}
				}
			}
		}

		if (predicate==null) {
			// Add time range, converting from milliseconds on QueryRequest to microseconds on Span
			Predicate end_Microseconds = Predicates.lessEqual("timestamp", request.endTs * 1000);
			Predicate start_Microseconds = Predicates.greaterEqual("timestamp", (request.endTs - request.lookback) * 1000);
			
			predicate = Predicates.and(end_Microseconds, start_Microseconds);
			
			// Add max duration, as min is derived by service by request.test() later.
			if (request.maxDuration!=null) {
				Predicate max = Predicates.lessEqual("duration", request.maxDuration);
				predicate = Predicates.and(predicate, max);
			}

			// String annotations
			if (request.annotations!=null && !request.annotations.isEmpty()) {
				for (String annotationStr : request.annotations) {
					if (annotationStr!=null && annotationStr.length() > 0) {
						String match = annotationStr.trim();
						if (match.length()>0) {
							Predicate annotationPredicate = Predicates.equal("annotations[any].value", annotationStr);
							Predicate binaryAnnotationPredicate =
									Predicates.equal("binaryAnnotations[any].key", annotationStr);
							predicate = Predicates.and(predicate, Predicates.or(annotationPredicate, binaryAnnotationPredicate));
						}
					}
				}
			}
			
			// Binary annotations, provided as key/value but value n
			if (request.binaryAnnotations!=null && !request.binaryAnnotations.isEmpty()) {
				for (Map.Entry<String, String> entry : request.binaryAnnotations.entrySet()) {
					predicate = Predicates.and(predicate, Predicates.equal("binaryAnnotations[any].key", entry.getKey()));
				}
			}
		}
		
		/* Search for matching spans
		 */
		Collection<Span> searchResult = new ArrayList<>();
		if (predicate!=null) {
			searchResult = spans.values(predicate);
		}

		/* Finally, prepare the output
		 */
	    List<List<Span>> result = new ArrayList<>();
	    
	    Collection<Span> theSpans = this._postProcess(searchResult);
	    
	    Set<Long> traceIdsInTimerange = this._traceIdsDescendingByTimestamp(theSpans);

	    for (Iterator<Long> traceId = traceIdsInTimerange.iterator();
	        traceId.hasNext() && result.size() < request.limit; ) {
	      Collection<Span> sameTraceId = this._spansByTraceId(theSpans, traceId.next());
	      for (List<Span> next : GroupByTraceId.apply(sameTraceId, strictTraceId, true)) {
	        if (request.test(next)) {
	          result.add(next);
	        }
	      }
	    }
	    
	    Collections.sort(result, TRACE_DESCENDING);
	    return result;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Span> getTrace(long traceIdHigh, long traceIdLow) {
		List<Span> result = this._getTrace(traceIdHigh, traceIdLow, IS_SORTED);
	    if (result==null) {
	    	return null;
	    }
	    return CorrectForClockSkew.apply(MergeById.apply(result));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Span> getRawTrace(long traceIdHigh, long traceIdLow) {
		return this._getTrace(traceIdHigh, traceIdLow, IS_NOT_SORTED);
	}

	/**
	 * <P>Retrieve spans for {@link #getTrace()} and {@link #getRawTrace()}.
	 * </P>
	 * <P>
	 * These calls expect a {@link java.util.List List} and Hazelcast returns
	 * a {@link java.util.Collection Collection} so convert efficiently.
	 * </P>
	 *
	 * @param traceIdHigh Only used in strict mode
	 * @param traceIdLow 
	 * @param sortRequired Whether or not to sort the result
	 * @return Null when no matches
	 */
	@SuppressWarnings("rawtypes")
	private List<Span> _getTrace(long traceIdHigh, long traceIdLow, boolean sortRequired) {

		Predicate predicate;
		if (this.strictTraceId) {
			predicate = Predicates.and(Predicates.equal("traceIdHigh", traceIdHigh),
					Predicates.equal("traceId", traceIdLow));
		} else {
			predicate = Predicates.equal("traceId", traceIdLow);
		}

		Collection<Span> collection = this.spans.values(predicate);

		if (collection.size()==0) {
			return null;
		}

		List<Span> list;

		if (collection instanceof List) {
			list = (List<Span>) collection;
		} else {
			list = new ArrayList<>(collection);
		}

		if (sortRequired) {
			Collections.sort(list);
		}

		return list;
	}
	
	/**
	 * @deprecated
	 * @{@inheritDoc}
	 */
	@Override
	public List<Span> getTrace(long traceId) {
	    return getTrace(0L, traceId);
	}

	/**
	 * @deprecated
	 * @{@inheritDoc}
	 */
	@Override
	public List<Span> getRawTrace(long traceId) {
	    return getRawTrace(0L, traceId);
	}

	/**
	 * @{@inheritDoc}
	 * <P>Return the mapping of service names to spans maintained by
	 * {@link HazelcastIMDGSpanConsumer#accept} when spans are inserted.
	 * </P>
	 * @return All services
	 */
	@Override
	public List<String> getServiceNames() {
		Collection<String> result = this.services.keySet();
		
		if (result==null||result.isEmpty()) {
			return Collections.emptyList();
		} else {
			return sortedList(result);
		}
	}

	/**
	 * @{@inheritDoc}
	 * <P>Return the mapping of service names to spans maintained by
	 * {@link HazelcastIMDGSpanConsumer#accept} when spans are inserted.
	 * </P>
	 * 
	 * @param serviceName A string
	 * @return Matching services
	 */
	@Override
	public List<String> getSpanNames(String serviceName) {
		if (serviceName==null || serviceName.length()==0) {
			return Collections.emptyList();
		}
		String inputServiceName = this._cleanseString(serviceName);

		Collection<String> result = this.services.get(inputServiceName);

		if (result==null || result.size()==0) {
			return Collections.emptyList();
		} else {
			return sortedList(result);
		}
	}

	@Override
	public List<DependencyLink> getDependencies(long endTs, Long lookback) {
		long effectiveLookback = (lookback==null ? endTs : lookback);
	    QueryRequest request = QueryRequest.builder()
	            .endTs(endTs)
	            .lookback(effectiveLookback)
	            .limit(Integer.MAX_VALUE).build();

	    DependencyLinker linksBuilder = new DependencyLinker();
	    for (Collection<Span> trace : getTraces(request)) {
	      linksBuilder.putTrace(trace);
	    }

	    return linksBuilder.link();
	}

	/**
	 * <P>Helper function to lowercase strings.
	 * </P>
	 */
	private String _cleanseString(String s) {
		if (s==null) {
			return null;
		}
		String result = s.toLowerCase().trim();
		return (result.length()==0 ? null : result);
	}

	/**
	 * <P>Hazelcast search results are unsorted, as they occur in parallel across
	 * multiple members. Sort the results, so the newest <I>n</I> trace Ids can
	 * be selected.
	 * </P>
	 * 
	 * @param searchResult From Hazelcast search
	 * @return TraceIds in the sequence of their unreturned timestamp.
	 */
	private Set<Long> _traceIdsDescendingByTimestamp(Collection<Span> searchResult) {
		if (searchResult.isEmpty()) {
			return Collections.emptySet();
		}
		
		TreeSet<Pair<Long>> matches = new TreeSet<>(VALUE_1_DESCENDING);

		for(Span span : searchResult) {
			if (span.timestamp!=null) {
				Pair<Long> pair = Pair.create(span.timestamp, span.traceId);
				matches.add(pair);
			}
		}
		
		// toCollection(LinkedHashSet::new) preserves order
		Set<Long> result = 
				matches.stream().map(pair -> pair._2)
				.collect(Collectors.toCollection(LinkedHashSet::new));
		return result;
	}

	/**
	 * <P>Borrowed from {@link zipkin.storage.InMemorySpanStore}
	 * </P>
	 */
	static final Comparator<Pair<Long>> VALUE_1_DESCENDING = (left, right) -> {
		    int result = right._1.compareTo(left._1);
		    if (result != 0) return result;
		    return right._2.compareTo(left._2);
	};

	/**
	 * <P>Post processing, some logic is supposed to be applied to query results rather
	 * than to data when it is stored.
	 * 
	 * @param searchResult From Hazelcast
	 * @return
	 */
	private Collection<Span> _postProcess(Collection<Span> searchResult) {
		Collection<Span> result = new ArrayList<>();
		for (Span span : searchResult) {
			// To be added at query time rather than to stored content
			if (span.timestamp==null & span.duration==null && span.annotations!=null && span.annotations.size() > 1) {
				TreeSet<Annotation> annotations = new TreeSet<>(span.annotations);
				long startTimestamp = annotations.first().timestamp;
				long endTimestamp = annotations.last().timestamp;
				span = span.toBuilder().duration(endTimestamp - startTimestamp).timestamp(startTimestamp).build();
			}
			
			result.add(span);
		}
		return result;
	}

	
	/**
	 * <P>Extract selected spans from search. Could probably replace with a lambda
	 * </P>
	 *
	 * @param theSpans A collection of spans
	 * @param traceId To look for
	 * @return The matches
	 */
	private Collection<Span> _spansByTraceId(Collection<Span> theSpans, long traceIdRequired) {
		Collection<Span> result = new ArrayList<>();
		for (Span span : theSpans) {
			if (span.traceId == traceIdRequired) {
				result.add(span);
			}
		}
		return result;
	}

}
