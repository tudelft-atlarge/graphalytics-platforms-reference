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
import it.unimi.dsi.fastutil.longs.LongSet;
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import nl.tudelft.graphalytics.util.graph.PropertyGraph;

/**
 * Reference implementation of the Single Source Shortest Path algorithm.
 *
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathJob {

	private static final Logger LOG = LogManager.getLogger();

	private static final double MAX_DISTANCE = Double.POSITIVE_INFINITY;

	private final PropertyGraph<Void, Double> graph;
	private final SingleSourceShortestPathsParameters parameters;

	public SingleSourceShortestPathJob(PropertyGraph<Void, Double> graph, SingleSourceShortestPathsParameters parameters) {
		this.graph = graph;
		this.parameters = parameters;
	}

	public Long2DoubleMap run() {
		// This method presents a basic implementation of Dijkstra's shortest path algorithm.
		LOG.debug("- Starting Single Source Shortest Path algorithm");

		// Define data structures
		Long2DoubleMap distances = new Long2DoubleOpenHashMap();
		LongSet visited = new LongLinkedOpenHashSet();
		LongSet pending = new LongLinkedOpenHashSet();

		// Initialize distances
		for (PropertyGraph<Void, Double>.Vertex v: graph.getVertices()) {
			distances.put(v.getId(), MAX_DISTANCE);
		}

		// Insert source vertex
		distances.put(parameters.getSourceVertex(), 0.0);
		pending.add(parameters.getSourceVertex());

		// Iterate until pending set is empty
		while (!pending.isEmpty()) {
			long minVertex = -1;
			double minDist = MAX_DISTANCE;

			// Find vertex in pending set for which distance is minimal
			for (long v: pending) {
				if (distances.get(v) < minDist) {
					minVertex = v;
					minDist = distances.get(v);
				}
			}

			// move vertex from pending set to visited set
			pending.remove(minVertex);
			visited.add(minVertex);

			// Inform the neighbors of this vertex
			for (PropertyGraph<Void, Double>.Edge edge: graph.getVertex(minVertex).getOutgoingEdges()) {
				long neighbor = edge.getDestinationVertex().getId();
				double edgeDist = edge.getValue();
				double newDist = minDist + edgeDist;

				// If neighbor not in pending set or distance has improved
				if (newDist < distances.get(neighbor)) {
					pending.add(neighbor);
					distances.put(neighbor, newDist);
				}
			}
		}

		LOG.debug("- Finished Single Source Shortest Path algorithm");

		return distances;
	}
}
