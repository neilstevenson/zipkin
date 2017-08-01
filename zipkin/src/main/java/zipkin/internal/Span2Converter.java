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
package zipkin.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.internal.Span2.Kind;

import static zipkin.BinaryAnnotation.Type.BOOL;
import static zipkin.BinaryAnnotation.Type.STRING;
import static zipkin.Constants.CLIENT_ADDR;
import static zipkin.Constants.LOCAL_COMPONENT;
import static zipkin.Constants.SERVER_ADDR;

/**
 * This converts {@link zipkin.Span} instances to {@link zipkin.internal.Span2} and visa versa.
 */
public final class Span2Converter {

  /**
   * Converts the input, parsing RPC annotations into {@link Span2#kind()}.
   *
   * @return a span for each unique {@link Annotation#endpoint annotation endpoint} service name.
   */
  public static List<Span2> fromSpan(Span source) {
    Builders builders = new Builders(source);
    // add annotations unless they are "core"
    builders.processAnnotations(source);
    // convert binary annotations to tags and addresses
    builders.processBinaryAnnotations(source);
    return builders.build();
  }

  static final class Builders {
    final List<Span2.Builder> spans = new ArrayList<>();
    Annotation cs = null, sr = null, ss = null, cr = null;

    Builders(Span source) {
      this.spans.add(newBuilder(source));
    }

    void processAnnotations(Span source) {
      for (int i = 0, length = source.annotations.size(); i < length; i++) {
        Annotation a = source.annotations.get(i);
        Span2.Builder currentSpan = forEndpoint(source, a.endpoint);
        // core annotations require an endpoint. Don't give special treatment when that's missing
        if (a.value.length() == 2 && a.endpoint != null) {
          if (a.value.equals(Constants.CLIENT_SEND)) {
            currentSpan.kind(Kind.CLIENT);
            cs = a;
          } else if (a.value.equals(Constants.SERVER_RECV)) {
            currentSpan.kind(Kind.SERVER);
            sr = a;
          } else if (a.value.equals(Constants.SERVER_SEND)) {
            currentSpan.kind(Kind.SERVER);
            ss = a;
          } else if (a.value.equals(Constants.CLIENT_RECV)) {
            currentSpan.kind(Kind.CLIENT);
            cr = a;
          } else {
            currentSpan.addAnnotation(a.timestamp, a.value);
          }
        } else {
          currentSpan.addAnnotation(a.timestamp, a.value);
        }
      }

      if (cs != null && sr != null) {
        // in a shared span, the client side owns span duration by annotations or explicit timestamp
        maybeTimestampDuration(source, cs, cr);

        // special-case loopback: We need to make sure on loopback there are two span2s
        Span2.Builder client = forEndpoint(source, cs.endpoint);
        Span2.Builder server;
        if (closeEnough(cs.endpoint, sr.endpoint)) {
          client.kind(Kind.CLIENT);
          // fork a new span for the server side
          server = newSpanBuilder(source, sr.endpoint).kind(Kind.SERVER);
        } else {
          server = forEndpoint(source, sr.endpoint);
        }

        // the server side is smaller than that, we have to read annotations to find out
        server.shared(true).timestamp(sr.timestamp);
        if (ss != null) server.duration(ss.timestamp - sr.timestamp);
        if (cr == null && source.duration == null) client.duration(null); // one-way has no duration
      } else if (cs != null && cr != null) {
        maybeTimestampDuration(source, cs, cr);
      } else if (sr != null && ss != null) {
        maybeTimestampDuration(source, sr, ss);
      } else { // otherwise, the span is incomplete. revert special-casing
        for (Span2.Builder next : spans) {
          if (Kind.CLIENT.equals(next.kind)) {
            if (cs != null) next.timestamp(cs.timestamp);
          } else if (Kind.SERVER.equals(next.kind)) {
            if (sr != null) next.timestamp(sr.timestamp);
          }
        }
        revertCoreAnnotation(source, ss);
        revertCoreAnnotation(source, cr);

        if (source.timestamp != null) {
          spans.get(0).timestamp(source.timestamp).duration(source.duration);
        }
      }
    }

    void revertCoreAnnotation(Span source, Annotation a) {
      if (a == null) return;
      forEndpoint(source, a.endpoint).kind(null).addAnnotation(a.timestamp, a.value);
    }

    void maybeTimestampDuration(Span source, Annotation begin, @Nullable Annotation end) {
      Span2.Builder span2 = forEndpoint(source, begin.endpoint);
      if (source.timestamp != null && source.duration != null) {
        span2.timestamp(source.timestamp).duration(source.duration);
      } else {
        span2.timestamp(begin.timestamp);
        if (end != null) span2.duration(end.timestamp - begin.timestamp);
      }
    }

    void processBinaryAnnotations(Span source) {
      Endpoint ca = null, sa = null;
      for (int i = 0, length = source.binaryAnnotations.size(); i < length; i++) {
        BinaryAnnotation b = source.binaryAnnotations.get(i);
        if (b.type == BOOL) {
          if (Constants.CLIENT_ADDR.equals(b.key)) {
            ca = b.endpoint;
          } else if (Constants.SERVER_ADDR.equals(b.key)) {
            sa = b.endpoint;
          }
          continue;
        }
        Span2.Builder currentSpan = forEndpoint(source, b.endpoint);
        if (b.type == STRING) {
          // don't add marker "lc" tags
          if (Constants.LOCAL_COMPONENT.equals(b.key) && b.value.length == 0) continue;
          currentSpan.putTag(b.key, new String(b.value, Util.UTF_8));
        }
      }

      if (cs != null && sa != null && !closeEnough(sa, cs.endpoint)) {
        forEndpoint(source, cs.endpoint).remoteEndpoint(sa);
      }

      if (sr != null && ca != null && !closeEnough(ca, sr.endpoint)) {
        forEndpoint(source, sr.endpoint).remoteEndpoint(ca);
      }

      // special-case when we are missing core annotations, but we have both address annotations
      if ((cs == null && sr == null) && (ca != null && sa != null)) {
        forEndpoint(source, ca).remoteEndpoint(sa);
      }
    }

    Span2.Builder forEndpoint(Span source, @Nullable Endpoint e) {
      if (e == null) return spans.get(0); // allocate missing endpoint data to first span
      for (int i = 0, length = spans.size(); i < length; i++) {
        Span2.Builder next = spans.get(i);
        if (next.localEndpoint == null) {
          next.localEndpoint = e;
          return next;
        } else if (closeEnough(next.localEndpoint, e)) {
          return next;
        }
      }
      return newSpanBuilder(source, e);
    }

    Span2.Builder newSpanBuilder(Span source, Endpoint e) {
      Span2.Builder result = newBuilder(source).localEndpoint(e);
      spans.add(result);
      return result;
    }

    List<Span2> build() {
      int length = spans.size();
      if (length == 1) return Collections.singletonList(spans.get(0).build());
      List<Span2> result = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        result.add(spans.get(i).build());
      }
      return result;
    }
  }

  static boolean closeEnough(Endpoint left, Endpoint right) {
    return left.serviceName.equals(right.serviceName);
  }

  static Span2.Builder newBuilder(Span source) {
    return Span2.builder()
      .traceIdHigh(source.traceIdHigh)
      .traceId(source.traceId)
      .parentId(source.parentId)
      .id(source.id)
      .name(source.name)
      .debug(source.debug);
  }

  /** Converts the input, parsing {@link Span2#kind()} into RPC annotations. */
  public static Span toSpan(Span2 in) {
    Span.Builder result = Span.builder()
      .traceIdHigh(in.traceIdHigh())
      .traceId(in.traceId())
      .parentId(in.parentId())
      .id(in.id())
      .debug(in.debug())
      .name(in.name() == null ? "" : in.name()); // avoid a NPE

    long timestamp = in.timestamp() == null ? 0L : in.timestamp();
    long duration = in.duration() == null ? 0L : in.duration();
    if (timestamp != 0L) {
      result.timestamp(timestamp);
      if (duration != 0L) result.duration(duration);
    }

    Annotation cs = null, sr = null, ss = null, cr = null;
    String remoteEndpointType = null;

    if (in.kind() != null) {
      switch (in.kind()) {
        case CLIENT:
          remoteEndpointType = Constants.SERVER_ADDR;
          if (timestamp != 0L) {
            cs = Annotation.create(timestamp, Constants.CLIENT_SEND, in.localEndpoint());
          }
          if (duration != 0L) {
            cr = Annotation.create(timestamp + duration, Constants.CLIENT_RECV, in.localEndpoint());
          }
          break;
        case SERVER:
          remoteEndpointType = Constants.CLIENT_ADDR;
          if (timestamp != 0L) {
            sr = Annotation.create(timestamp, Constants.SERVER_RECV, in.localEndpoint());
          }
          if (duration != 0L) {
            ss = Annotation.create(timestamp + duration, Constants.SERVER_SEND, in.localEndpoint());
          }
          break;
        default:
          throw new AssertionError("update kind mapping");
      }
    }

    boolean wroteEndpoint = false;

    for (int i = 0, length = in.annotations().size(); i < length; i++) {
      Annotation a = in.annotations().get(i);
      if (in.localEndpoint() != null) {
        a = a.toBuilder().endpoint(in.localEndpoint()).build();
      }
      if (a.value.length() == 2) {
        if (a.value.equals(Constants.CLIENT_SEND)) {
          cs = a;
          remoteEndpointType = SERVER_ADDR;
        } else if (a.value.equals(Constants.SERVER_RECV)) {
          sr = a;
          remoteEndpointType = CLIENT_ADDR;
        } else if (a.value.equals(Constants.SERVER_SEND)) {
          ss = a;
        } else if (a.value.equals(Constants.CLIENT_RECV)) {
          cr = a;
        } else {
          wroteEndpoint = true;
          result.addAnnotation(a);
        }
      } else {
        wroteEndpoint = true;
        result.addAnnotation(a);
      }
    }

    for (Map.Entry<String, String> tag : in.tags().entrySet()) {
      wroteEndpoint = true;
      result.addBinaryAnnotation(
        BinaryAnnotation.create(tag.getKey(), tag.getValue(), in.localEndpoint()));
    }

    if (cs != null || sr != null || ss != null || cr != null) {
      if (cs != null) result.addAnnotation(cs);
      if (sr != null) result.addAnnotation(sr);
      if (ss != null) result.addAnnotation(ss);
      if (cr != null) result.addAnnotation(cr);
      wroteEndpoint = true;
    } else if (in.localEndpoint() != null && in.remoteEndpoint() != null) {
      // special-case when we are missing core annotations, but we have both address annotations
      result.addBinaryAnnotation(BinaryAnnotation.address(CLIENT_ADDR, in.localEndpoint()));
      wroteEndpoint = true;
      remoteEndpointType = SERVER_ADDR;
    }

    if (remoteEndpointType != null && in.remoteEndpoint() != null) {
      result.addBinaryAnnotation(BinaryAnnotation.address(remoteEndpointType, in.remoteEndpoint()));
    }

    // don't report server-side timestamp on shared or incomplete spans
    if (Boolean.TRUE.equals(in.shared()) && sr != null) {
      result.timestamp(null).duration(null);
    }
    if (in.localEndpoint() != null && !wroteEndpoint) { // create a dummy annotation
      result.addBinaryAnnotation(BinaryAnnotation.create(LOCAL_COMPONENT, "", in.localEndpoint()));
    }
    return result.build();
  }
}
