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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.graph.PropertyList;
import nl.tudelft.graphalytics.domain.graph.PropertyType;
import nl.tudelft.graphalytics.validation.GraphStructure;

/**
 * Graph parser for Graphalytics's EVL graph format.
 *
 * @author Tim Hegeman
 */
public class GraphParser {
	private GraphParser reversedGraph;
	private GraphParser undirectedGraph;

	private final LongSet vertices;
	private final Long2ObjectMap<List<Object>> vertexProperties;

	private final Long2ObjectMap<LongList> edges;
	private final Long2ObjectMap<List<List<Object>>> edgeProperties;

	public GraphParser(LongSet vertices, Long2ObjectMap<List<Object>> vertexProperties,
			Long2ObjectMap<LongList> edges, Long2ObjectMap<List<List<Object>>> edgeProperties) {
		this.vertices = vertices;
		this.edges = edges;
		this.vertexProperties = vertexProperties;
		this.edgeProperties = edgeProperties;
	}

	/**
	 * Get number of vertices of this graph.
	 *
	 * @return The number of vertices.
	 */
	public int getNumberOfVertices() {
		return vertices.size();
	}

	/**
	 * Get the vertex ids of this graph.
	 *
	 * @return The vertex ids
	 */
	public LongSet getVertices() {
		return vertices;
	}

	/**
	 * Get the ids of the outgoing neighbors of a vertex.
	 *
	 * @param vertex The vertex to get the neighbors of
	 * @return The neighbor ids.
	 */
	public LongList getNeighbors(long vertex) {
		return edges.get(vertex);
	}

	/**
	 * Get the properties of a vertex
	 *
	 * @param vertex The vertex.
	 * @return The list of property values.
	 */
	public List<Object> getVertexProperties(long vertex) {
		return vertexProperties.get(vertex);
	}

	/**
	 * Get the edge properties of the outgoing edge towards the n-th neighbor of a vertex.
	 *
	 * @param vertex The source vertex of the edge.
	 * @param neighborIndex The index of the neighbor.
	 * @return The edge properties.
	 */
	public List<Object> getEdgeProperties(long vertex, int neighborIndex) {
		return edgeProperties.get(vertex).get(neighborIndex);
	}

	private GraphParser generateReverseGraph(boolean keepOriginal) {
		Long2ObjectMap<LongList> revEdges = new Long2ObjectOpenHashMap<>();
		Long2ObjectMap<List<List<Object>>> revEdgeProps = new Long2ObjectOpenHashMap<>();

		for (long v: vertices) {
			revEdges.put(v, new LongArrayList());
			revEdgeProps.put(v, new ArrayList<List<Object>>());
		}

		for (long v: vertices) {
			LongList neighbors = edges.get(v);
			List<List<Object>> neighborProps = edgeProperties.get(v);

			for (int i = 0; i < neighbors.size(); i++) {
				long u = neighbors.getLong(i);
				List<Object> props = neighborProps.get(i);

				if (keepOriginal) {
					revEdges.get(v).add(u);
					revEdgeProps.get(v).add(props);
				}

				revEdges.get(u).add(v);
				revEdgeProps.get(u).add(props);
			}
		}

		return new GraphParser(vertices, vertexProperties, revEdges, revEdgeProps);
	}

	/**
	 * Returns a reversed graph of the current graph (i.e., all edges are inverted).
	 *
	 * @return The reversed graph.
	 */
	public GraphParser toReversed() {
		if (reversedGraph == null) {
			reversedGraph = generateReverseGraph(false);
			reversedGraph.reversedGraph = this;
		}

		return reversedGraph;
	}

	/**
	 * Return an undirected graph of the current graph by duplicating all
	 * edges in both directions.
	 *
	 * @return The undirected graph
	 */
	public GraphParser toUndirected() {
		if (undirectedGraph == null) {
			undirectedGraph = generateReverseGraph(true);
			undirectedGraph.undirectedGraph = undirectedGraph;
		}

		return undirectedGraph;
	}

	private static void parseVertices(String file,
			PropertyList propTypes,
			LongSet vertices,
			Long2ObjectMap<List<Object>> vProp,
			Long2ObjectMap<LongList> edges,
			Long2ObjectMap<List<List<Object>>> eProp) throws IOException {

		try (BufferedReader r = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = r.readLine()) != null) {
				line = line.trim();

				if (line.isEmpty()) {
					continue;
				}

				String[] parts = line.split("[ \t]+");
				long id = Long.parseLong(parts[0]);

				vertices.add(id);
				vProp.put(id, parseProperties(propTypes, parts, 1));
				edges.put(id, new LongArrayList());
				eProp.put(id, new ArrayList<List<Object>>());
			}
		}
	}

	private static void parseEdges(String file,
			PropertyList propTypes,
			Long2ObjectMap<LongList> edges,
			Long2ObjectMap<List<List<Object>>> eProp) throws IOException {

		try (BufferedReader r = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = r.readLine()) != null) {
				line = line.trim();

				if (line.isEmpty()) {
					continue;
				}

				String[] parts = line.split("[ \t]+");
				long src = Long.parseLong(parts[0]);
				long target = Long.parseLong(parts[1]);

				edges.get(src).add(target);
				eProp.get(src).add(parseProperties(propTypes, parts, 2));
			}
		}
	}

	private static List<Object> parseProperties(PropertyList types, String[] parts, int offset) throws IOException {
		if (types.size() != parts.length - offset) {
			throw new IOException("error while parsing properties, "
					+ types.size() + " properties expected but got "
					+ (parts.length - offset) + " properties");
		}

		List<Object> props = new ArrayList<Object>();

		for (int i = 0; i < types.size(); i++) {
			String str = parts[offset + i];
			PropertyType type = types.get(i).getType();
			props.add(parseProperty(type, str));
		}

		return props;
	}

	private static Object parseProperty(PropertyType type, String str) throws IOException {
		if (PropertyType.INTEGER.equals(type)) {
			return Integer.parseInt(str);
		} else if (PropertyType.REAL.equals(type)) {
			return Double.parseDouble(str);
		} else {
			throw new IOException("Found unsupported property "
					+ "type while parsing " + type);
		}
	}

	/**
	 * Create a {@link nl.tudelft.graphalytics.reference.GraphParser} by parsing
	 * the provided {@link nl.tudelft.graphalytics.domain.Graph}
	 *
	 * @param graph The original graph
	 * @return The resulting graph.
	 * @throws IOException If an error occurs while reading the input files.
	 */
	public static GraphParser parseGraph(Graph graph) throws IOException {
		LongSet vertices = new LongOpenHashSet();
		Long2ObjectMap<List<Object>> vProp = new Long2ObjectOpenHashMap<>();
		Long2ObjectMap<LongList> edges = new Long2ObjectOpenHashMap<>();
		Long2ObjectMap<List<List<Object>>> eProp = new Long2ObjectOpenHashMap<>();

		parseVertices(
				graph.getVertexFilePath(), graph.getVertexProperties(),
				vertices, vProp,
				edges, eProp);

		parseEdges(
				graph.getEdgeFilePath(), graph.getEdgeProperties(),
				edges, eProp);

		GraphParser g = new GraphParser(vertices, vProp, edges, eProp);
		return graph.isDirected() ? g : g.toUndirected();
	}

	/**
	 * Convert a {@link nl.tudelft.graphalytics.validation.GraphStructure} to
	 * {@link nl.tudelft.graphalytics.reference.GraphParser}
	 *
	 * @param graph The original graph.
	 * @return The converted graph.
	 */
	public static GraphParser parseGraphStructure(GraphStructure graph) {
		LongSet vertices = new LongOpenHashSet();
		Long2ObjectMap<List<Object>> vProp = new Long2ObjectOpenHashMap<>();
		Long2ObjectMap<LongList> edges = new Long2ObjectOpenHashMap<>();
		Long2ObjectMap<List<List<Object>>> eProp = new Long2ObjectOpenHashMap<>();

		for (long v: graph.getVertices()) {
			vertices.add(v);
			vProp.put(v, Collections.emptyList());
			edges.put(v, new LongArrayList());
			eProp.put(v, new ArrayList<List<Object>>());

			for (long u: graph.getEdgesForVertex(v)) {
				edges.get(v).add(u);
				eProp.get(v).add(Collections.emptyList());
			}
		}

		return new GraphParser(vertices, vProp, edges, eProp);

	}
}
