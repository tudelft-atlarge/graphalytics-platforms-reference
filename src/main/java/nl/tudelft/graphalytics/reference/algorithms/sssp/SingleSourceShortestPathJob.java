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
package nl.tudelft.graphalytics.reference.algorithms.sssp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongSet;
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathParameters;
import nl.tudelft.graphalytics.reference.GraphParser;

/**
 * Reference implementation of the Single Source Shortest Path algorithm.
 *
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathJob {

	private static final Logger LOG = LogManager.getLogger();

	private static final double MAX_DISTANCE = Double.POSITIVE_INFINITY;

	private final GraphParser graph;
	private final SingleSourceShortestPathParameters parameters;

	public SingleSourceShortestPathJob(GraphParser graph, SingleSourceShortestPathParameters parameters) {
		this.graph = graph;
		this.parameters = parameters;
	}

	public Long2DoubleMap run() {
		// Basic implementation of Dijkstra's shortests path algorithm
		LOG.debug("- Starting Single Source Shortest Path algorithm");

		// Define data structures
		Long2DoubleMap distances = new Long2DoubleOpenHashMap();
		LongSet visited = new LongLinkedOpenHashSet();
		LongSet pending = new LongLinkedOpenHashSet();

		// Initialize distances
		for (long v: graph.getVertices()) {
			distances.put(v, MAX_DISTANCE);
		}

		// Insert source vertex
		distances.put(parameters.getSourceVertex(), 0.0);
		pending.add(parameters.getSourceVertex());

		// Iterate until pending set is empty
		while (!pending.isEmpty()) {
			long minVertex = -1;
			double minDistance = MAX_DISTANCE;

			// Find vertex in pending set for which distance is minimal
			for (long v: pending) {
				if (distances.get(v) < minDistance) {
					minVertex = v;
					minDistance = distances.get(v);
				}
			}

			// move vertex from pending set to visited set
			pending.remove(minVertex);
			visited.add(minVertex);

			// Inform the neighbors of this vertex
			LongList neighbors = graph.getNeighbors(minVertex);

			for (int i = 0; i < neighbors.size(); i++) {
				long neighbor = neighbors.getLong(i);

				if (!visited.contains(neighbor)) {
					double edgeDist = (Double) graph.getEdgeProperty(minVertex, i, 0);
					double newDist = minDistance + edgeDist;

					// If neighbor not in pending set or distance has improved
					if (!pending.contains(neighbor) || distances.get(neighbor) > newDist) {
						pending.add(neighbor);
						distances.put(neighbor, newDist);
					}
				}
			}
		}

		LOG.debug("- Finished Single Source Shortest Path algorithm");

		return distances;
	}
}
