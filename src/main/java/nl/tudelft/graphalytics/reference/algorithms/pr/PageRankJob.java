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
import nl.tudelft.graphalytics.reference.GraphParser;

/**
 * Reference implementation of PageRank algorithm.
 *
 * @author Tim Hegeman
 */
public class PageRankJob {
	private static final Logger LOG = LogManager.getLogger();

	private final GraphParser outGraph;
	private final GraphParser inGraph;
	private final PageRankParameters parameters;

	public PageRankJob(GraphParser graph, PageRankParameters parameters) {
		// Given graph only stores outgoing edges for each vertex. If graph is directed,
		// invert all edges to find ingoing edges of each vertex.
		this.inGraph = graph.toReversed();
		this.outGraph = graph;

		this.parameters = parameters;
	}

	public Long2DoubleMap run() {
		LOG.debug("- Starting PageRank algorithm");

		// Read parameters
		int numVertices = outGraph.getNumberOfVertices();
		int numIterations = parameters.getNumberOfIterations();
		double dampingFactor = parameters.getDampingFactor();

		// Initialize values
		Long2DoubleMap ranks = new Long2DoubleOpenHashMap(numVertices);
		Long2DoubleMap newRanks = new Long2DoubleOpenHashMap(numVertices);

		for (long v: outGraph.getVertices()) {
			ranks.put(v, 1.0 / numVertices);
		}

		// Run iterations
		for (int it = 0; it < numIterations; it++) {
			LOG.debug("- Iteration " +  it);

			double danglingSum = 0.0;

			// Collect sum of ranks for dangling vertices (i.e., without outgoing edges)
			for (long v: outGraph.getVertices()) {
				if (outGraph.getNeighbors(v).isEmpty()) {
					danglingSum += ranks.get(v);
				}
			}

			// Compute new rank for all vertices
			for (long v: outGraph.getVertices()) {
				double sum = 0.0;

				for (long neighbor: inGraph.getNeighbors(v)) {
					sum += ranks.get(neighbor) / outGraph.getNeighbors(neighbor).size();
				}

				double newRank = (1.0 - dampingFactor) / numVertices
						+ dampingFactor * (sum + danglingSum / numVertices);

				newRanks.put(v, newRank);
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
