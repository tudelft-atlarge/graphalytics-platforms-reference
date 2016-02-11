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
package nl.tudelft.graphalytics.reference.algorithms.pr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.util.graph.PropertyGraph;

/**
 * Reference implementation of PageRank algorithm.
 *
 * @author Tim Hegeman
 */
public class PageRankJob {
	private static final Logger LOG = LogManager.getLogger();

	private final PropertyGraph<Void, Void> graph;
	private final PageRankParameters parameters;

	public PageRankJob(PropertyGraph<Void, Void> graph, PageRankParameters parameters) {
		this.graph = graph;
		this.parameters = parameters;
	}

	public Long2DoubleMap run() {
		LOG.debug("- Starting PageRank algorithm");

		// Read parameters
		int numVertices = graph.getVertices().size();
		int numIterations = parameters.getNumberOfIterations();
		double dampingFactor = parameters.getDampingFactor();

		// Initialize values
		Long2DoubleMap ranks = new Long2DoubleOpenHashMap(numVertices);
		Long2DoubleMap newRanks = new Long2DoubleOpenHashMap(numVertices);

		for (PropertyGraph<Void, Void>.Vertex v: graph.getVertices()) {
			ranks.put(v.getId(), 1.0 / numVertices);
		}

		// Run iterations
		for (int it = 0; it < numIterations; it++) {
			LOG.debug("- Iteration " +  it);

			double danglingSum = 0.0;

			// Collect sum of ranks for dangling vertices (i.e., without outgoing edges)
			for (PropertyGraph<Void, Void>.Vertex v: graph.getVertices()) {
				if (v.getOutgoingEdges().isEmpty()) {
					danglingSum += ranks.get(v.getId());
				}
			}

			// Compute new rank for all vertices
			for (PropertyGraph<Void, Void>.Vertex v: graph.getVertices()) {
				double sum = 0.0;

				for (PropertyGraph<Void, Void>.Edge e: v.getIncomingEdges()) {
					PropertyGraph<Void, Void>.Vertex u = e.getSourceVertex();
					sum += ranks.get(u.getId()) / u.getOutgoingEdges().size();
				}

				double newRank = (1.0 - dampingFactor) / numVertices
						+ dampingFactor * (sum + danglingSum / numVertices);

				newRanks.put(v.getId(), newRank);
			}

			// Swap prev and next
			Long2DoubleMap tmp = ranks;
			ranks = newRanks;
			newRanks = tmp;
		}

		LOG.debug("- Finished PageRank algorithm");

		return ranks;
	}
}
