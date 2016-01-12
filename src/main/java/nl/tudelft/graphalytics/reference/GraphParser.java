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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import nl.tudelft.graphalytics.domain.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Graph parser for Graphalytics's EVL graph format.
 *
 * @author Tim Hegeman
 */
public class GraphParser {

	private final String vertexFilePath;
	private final String edgeFilePath;
	private final boolean graphIsDirected;
	private Long2ObjectMap<LongList> graphData;

	public GraphParser(Graph graph) {
		this.vertexFilePath = graph.getVertexFilePath();
		this.edgeFilePath = graph.getEdgeFilePath();
		this.graphIsDirected = graph.getGraphFormat().isDirected();
		this.graphData = null;
	}

	/**
	 * Performs the parsing operation on the graph specified in the class constructor. The result is an adjacency list
	 * representation of the graph, i.e. for each vertex v a list is constructed with the ids of all vertices for which
	 * an edge from v to that vertex exists.
	 *
	 * @return an adjacency list representation of the graph
	 * @throws IOException
	 */
	public Long2ObjectMap<LongList> parse() throws IOException {
		if (graphData != null) {
			return graphData;
		}

		graphData = new Long2ObjectOpenHashMap<>();
		parseVertices();
		parseEdges();
		return graphData;
	}

	private void parseVertices() throws IOException {
		try (BufferedReader vertexReader = new BufferedReader(new FileReader(vertexFilePath))) {
			String line = vertexReader.readLine();
			while (line != null) {
				if (!line.isEmpty()) {
					graphData.put(Long.parseLong(line), new LongArrayList());
				}
				line = vertexReader.readLine();
			}
		}
	}

	private void parseEdges() throws IOException {
		try (BufferedReader edgeReader = new BufferedReader(new FileReader(edgeFilePath))) {
			String line = edgeReader.readLine();
			while (line != null) {
				if (!line.isEmpty()) {
					String[] tokens = line.split(" ");
					long source = Long.parseLong(tokens[0]);
					long destination = Long.parseLong(tokens[1]);
					graphData.get(source).add(destination);
					
					// Add edge in both directions if undirected
					if (!graphIsDirected) {
						graphData.get(destination).add(source);
					}
				}
				line = edgeReader.readLine();
			}
		}
	}
	
	static public Long2ObjectMap<LongList> convertToUndirected(Long2ObjectMap<LongList> graphData) {
		Long2ObjectMap<LongList> newGraphData = new Long2ObjectOpenHashMap<>(graphData.size());

		for (long source: graphData.keySet()) {
			newGraphData.put(source, new LongArrayList());
		}
		
		for (long source: graphData.keySet()) {
			for (long destination: graphData.get(source)) {
				newGraphData.get(source).add(destination);
				newGraphData.get(destination).add(source);
			}
		}
		
		return newGraphData;
	}
}
