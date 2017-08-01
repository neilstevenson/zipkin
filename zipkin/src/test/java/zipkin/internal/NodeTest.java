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
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import zipkin.Span;
import zipkin.TestObjects;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class NodeTest {
  List<String> messages = new ArrayList<>();

  Logger logger = new Logger("", null) {
    {
      setLevel(Level.ALL);
    }

    @Override public void log(Level level, String msg) {
      assertThat(level).isEqualTo(Level.FINE);
      messages.add(msg);
    }
  };

  @Test(expected = NullPointerException.class)
  public void addValue_nullNotAllowed() {
    new Node<>().value(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void addChild_selfNotAllowed() {
    Node<Character> a = new Node<Character>().value('a');
    a.addChild(a);
  }

  /**
   * <p>The following tree should traverse in alphabetical order <pre>{@code
   *
   *          a
   *        / | \
   *       b  c  d
   *      /|\     \
   *     e f g     h
   * }</pre>
   */
  @Test
  public void traversesBreadthFirst() {
    Node<Character> a = new Node<Character>().value('a');
    Node<Character> b = new Node<Character>().value('b');
    Node<Character> c = new Node<Character>().value('c');
    Node<Character> d = new Node<Character>().value('d');
    // root(a) has children b, c, d
    a.addChild(b).addChild(c).addChild(d);
    Node<Character> e = new Node<Character>().value('e');
    Node<Character> f = new Node<Character>().value('f');
    Node<Character> g = new Node<Character>().value('g');
    // child(b) has children e, f, g
    b.addChild(e).addChild(f).addChild(g);
    Node<Character> h = new Node<Character>().value('h');
    // f has no children
    // child(g) has child h
    g.addChild(h);

    assertThat(a.traverse()).extracting(Node::value)
        .containsExactly('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h');
  }

  /**
   * Makes sure that the trace tree is constructed based on parent-child, not by parameter order.
   */
  @Test
  public void constructsTraceTree() {
    // TRACE is sorted with root span first, lets shuffle them to make
    // sure the trace is stitched together by id.
    List<Span> copy = new ArrayList<>(TestObjects.TRACE);

    Collections.shuffle(copy);

    Node.TreeBuilder<Span> treeBuilder =
      new Node.TreeBuilder<>(logger, copy.get(0).traceIdString());
    for (Span span : copy) {
      treeBuilder.addNode(span.parentId, span.id, span);
    }
    Node<Span> root = treeBuilder.build();
    assertThat(root.value())
        .isEqualTo(TestObjects.TRACE.get(0));

    assertThat(root.children()).extracting(Node::value)
        .containsExactly(TestObjects.TRACE.get(1));

    Node<Span> child = root.children().iterator().next();
    assertThat(child.children()).extracting(Node::value)
        .containsExactly(TestObjects.TRACE.get(2));
  }

  @Test
  public void constructTree_noChildLeftBehind() {
    List<Span> spans = asList(
      Span.builder().traceId(137L).id(1L).name("root-0").build(),
      Span.builder().traceId(137L).parentId(1L).id(2L).name("child-0").build(),
      Span.builder().traceId(137L).parentId(1L).id(3L).name("child-1").build(),
      Span.builder().traceId(137L).id(4L).name("lost-0").build(),
      Span.builder().traceId(137L).id(5L).name("lost-1").build());
    int treeSize = 0;
    Node.TreeBuilder<Span> treeBuilder =
      new Node.TreeBuilder<>(logger, spans.get(0).traceIdString());
    for (Span span : spans) {
      assertThat(treeBuilder.addNode(span.parentId, span.id, span))
        .isTrue();
    }
    Node<Span> tree = treeBuilder.build();
    Iterator<Node<Span>> iter = tree.traverse();
    while (iter.hasNext()) {
      iter.next();
      treeSize++;
    }
    assertThat(treeSize).isEqualTo(spans.size());
    assertThat(messages).containsExactly(
      "attributing span missing parent to root: traceId=0000000000000089, rootSpanId=0000000000000001, spanId=0000000000000004",
      "attributing span missing parent to root: traceId=0000000000000089, rootSpanId=0000000000000001, spanId=0000000000000005"
    );
  }

  @Test public void constructTree_headless() {
    Span s2 = Span.builder().traceId(137L).parentId(1L).id(2L).name("s2").build();
    Span s3 = Span.builder().traceId(137L).parentId(1L).id(3L).name("s3").build();
    Span s4 = Span.builder().traceId(137L).parentId(1L).id(4L).name("s4").build();

    Node.TreeBuilder<Span> treeBuilder = new Node.TreeBuilder<>(logger, s2.traceIdString());
    for (Span span : asList(s2, s3, s4)) {
      treeBuilder.addNode(span.parentId, span.id, span);
    }
    Node<Span> root = treeBuilder.build();
    assertThat(root.isSyntheticRootForPartialTree())
      .isTrue();
    assertThat(root.children()).extracting(Node::value)
      .containsExactly(s2, s3, s4);
    assertThat(messages).containsExactly(
      "substituting dummy node for missing root span: traceId=0000000000000089"
    );
  }

  @Test
  public void addNode_skipsOnCycle() {
    Span s1 = Span.builder().traceId(137L).parentId(null).id(1L).name("s1").build();
    Span s2 = Span.builder().traceId(137L).parentId(2L).id(2L).name("s2").build();

    Node.TreeBuilder<Span> treeBuilder = new Node.TreeBuilder<>(logger, s2.traceIdString());
    treeBuilder.addNode(s1.parentId, s1.id, s1);
    assertThat(treeBuilder.addNode(s2.parentId, s2.id, s2)).isFalse();

    treeBuilder.build();
    assertThat(messages).containsExactly(
      "skipping circular dependency: traceId=0000000000000089, spanId=0000000000000002"
    );
  }
}
