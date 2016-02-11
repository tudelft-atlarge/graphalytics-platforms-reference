/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.tudelft.graphalytics.reference;

import java.io.IOException;
import java.util.Iterator;

import nl.tudelft.graphalytics.util.graph.PropertyGraph;
import nl.tudelft.graphalytics.util.graph.PropertyGraphParser;
import nl.tudelft.graphalytics.util.graph.PropertyGraphParser.ValueParser;
import nl.tudelft.graphalytics.util.io.EdgeListStream;
import nl.tudelft.graphalytics.util.io.VertexListStream;
import nl.tudelft.graphalytics.validation.GraphStructure;

/**
 * Parser for the validation graph structure used by the Graphalytics validation framework.
 *
 * @author Tim Hegeman
 */
public final class Util {

	static class VoidValueParser implements ValueParser<Void> {
		@Override
		public Void parse(String[] valueTokens) throws IOException {
			return null;
		}
	}

	static class VertexStream implements VertexListStream {
		final private Iterator<Long> iterator;

		public VertexStream(GraphStructure graph) {
			iterator = graph.getVertices().iterator();
		}

		@Override
		public boolean hasNextVertex() throws IOException {
			return iterator.hasNext();
		}

		@Override
		public VertexData getNextVertex() throws IOException {
			return new VertexData(iterator.next(), new String[0]);
		}

		@Override
		public void close() throws IOException {
			//
		}
	}

	static class EdgeStream implements EdgeListStream {
		final private GraphStructure graph;
		final private Iterator<Long> vertexIterator;
		private Iterator<Long> edgeIterator;
		private long currentVertexId;

		public EdgeStream(GraphStructure graph) {
			this.graph = graph;
			this.vertexIterator = graph.getVertices().iterator();
			this.currentVertexId = vertexIterator.next();
			this.edgeIterator = graph.getEdgesForVertex(currentVertexId).iterator();
		}

		@Override
		public boolean hasNextEdge() throws IOException {
			while (!edgeIterator.hasNext()) {
				if (!vertexIterator.hasNext()) {
					return false;
				}

				currentVertexId = vertexIterator.next();
				edgeIterator = graph.getEdgesForVertex(currentVertexId).iterator();
			}

			return true;
		}

		@Override
		public EdgeData getNextEdge() throws IOException {
			return new EdgeData(currentVertexId, edgeIterator.next(), new String[0]);
		}

		@Override
		public void close() throws IOException {
			// empty
		}
	}

	public static PropertyGraph<Void, Void> convertToPropertyGraph(GraphStructure graph) {
		try {
			return PropertyGraphParser.parsePropertyGraph(
					new VertexStream(graph),
					new EdgeStream(graph),
					true,
					new VoidValueParser(),
					new VoidValueParser());
		} catch(IOException e) {
			throw new IllegalStateException("Should never happen", e);
		}
	}
}
