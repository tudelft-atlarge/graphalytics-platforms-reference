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
package nl.tudelft.graphalytics.reference.algorithms.bfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.AbstractLongPriorityQueue;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.reference.GraphParser;

/**
 * Reference implementation of the Breadth First Search algorithm.
 *
 * @author Tim Hegeman
 */
public class BreadthFirstSearchJob {

	private static final Logger LOG = LogManager.getLogger();

	private static final long MAX_DISTANCE = Long.MAX_VALUE;

	private final GraphParser graph;
	private final BreadthFirstSearchParameters parameters;

	public BreadthFirstSearchJob(GraphParser graph, BreadthFirstSearchParameters parameters) {
		this.graph = graph;
		this.parameters = parameters;
	}

	public Long2LongMap run() {
		LOG.debug("- Starting Breadth First Search algorithm");

		// Initialize distances
		Long2LongMap distances = new Long2LongOpenHashMap(graph.getNumberOfVertices());
		for (long v: graph.getVertices()) {
			distances.put(v, MAX_DISTANCE);
		}
		distances.put(parameters.getSourceVertex(), 0L);

		// Define visited set
		LongSet visited = new LongOpenHashSet();
		visited.add(parameters.getSourceVertex());

		// Define traversal queue
		AbstractLongPriorityQueue queue = new LongArrayFIFOQueue();
		queue.enqueue(parameters.getSourceVertex());

		// Traverse the graph
		while (!queue.isEmpty()) {
			long currentVertexId = queue.dequeueLong();
			long currentVertexDistance = distances.get(currentVertexId);

			// Iterate over all outgoing edges of this vertex
			for (long neighbour: graph.getNeighbors(currentVertexId)) {

				// If a neighbour has not been visited, add it to the queue and set its distance from the root
				if (!visited.contains(neighbour)) {
					visited.add(neighbour);
					queue.enqueue(neighbour);
					distances.put(neighbour, currentVertexDistance + 1);
				}
			}
		}

		LOG.debug("- Finished Breadth First Search algorithm");

		return distances;
	}
}
