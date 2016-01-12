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

import it.unimi.dsi.fastutil.longs.*;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.reference.GraphParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Reference implementation of the Breadth First Search algorithm.
 *
 * @author Tim Hegeman
 */
public class BreadthFirstSearchJob {

	private static final Logger LOG = LogManager.getLogger();

	private static final long MAX_DISTANCE = Long.MAX_VALUE;

	private final Long2ObjectMap<LongList> graphData;
	private final BreadthFirstSearchParameters parameters;

	public BreadthFirstSearchJob(Long2ObjectMap<LongList> graphData, boolean directed, BreadthFirstSearchParameters parameters) {
		this.graphData = !directed ? GraphParser.convertToUndirected(graphData) : graphData;
		this.parameters = parameters;
	}

	public Long2LongMap run() {
		LOG.debug("- Starting Breadth First Search algorithm");

		// Initialize distances
		Long2LongMap distances = new Long2LongOpenHashMap(graphData.size());
		for (long v: graphData.keySet()) {
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
			for (LongIterator neighbourIterator = graphData.get(currentVertexId).iterator(); neighbourIterator.hasNext(); ) {
				long neighbour = neighbourIterator.nextLong();
				
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
