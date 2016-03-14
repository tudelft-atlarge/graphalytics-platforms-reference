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

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

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
		Long2DoubleMap distances = new Long2DoubleOpenHashMap(); // for O(1) lookup for neighbors' distance
		PriorityQueue<Vertex> queue = new PriorityQueue<>(graph.getVertices().size(), new Vertex()); // for O(log n) lookup for priority
		LongSet visited = new LongLinkedOpenHashSet(); // for tracking duplicated queue items.

		// Initialize distances
		for (PropertyGraph<Void, Double>.Vertex v : graph.getVertices()) {
			distances.put(v.getId(), MAX_DISTANCE);
		}

		// Insert source vertex
		distances.put(parameters.getSourceVertex(), 0.0);
		queue.add(new Vertex(parameters.getSourceVertex(), 0.0));

		// Iterate until pending set is empty
		while (!queue.isEmpty()) {
			Vertex minVertex = queue.remove();


			// Inform the neighbors of this vertex
			if (!visited.contains(minVertex.getId())) {
				visited.add(minVertex.getId());

				for (PropertyGraph<Void, Double>.Edge edge : graph.getVertex(minVertex.getId()).getOutgoingEdges()) {
					long neighbor = edge.getDestinationVertex().getId();
					double edgeDist = edge.getValue();
					double newDist = minVertex.getDist() + edgeDist;

					// If neighbor not in pending set or distance has improved
					if (newDist < distances.get(neighbor)) {
						queue.add(new Vertex(neighbor, newDist));
						distances.put(neighbor, newDist);
					}
				}
			}
		}

		LOG.debug("- Finished Single Source Shortest Path algorithm");

		return distances;
	}

	private class Vertex implements Comparator<Vertex> {
		private long id;
		private double dist;

		public Vertex() {
		}

		public Vertex(long id, double dist) {
			this.id = id;
			this.dist = dist;
		}

		@Override
		public int compare(Vertex v1, Vertex v2) {
			if (v1.dist < v2.dist)
				return -1;
			if (v1.dist > v2.dist)
				return 1;
			return 0;
		}

		public long getId() {
			return id;
		}

		public double getDist() {
			return dist;
		}

	}
}
