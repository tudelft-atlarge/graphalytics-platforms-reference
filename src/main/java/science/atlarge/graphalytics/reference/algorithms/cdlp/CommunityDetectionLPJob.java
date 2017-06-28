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
package science.atlarge.graphalytics.reference.algorithms.cdlp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import science.atlarge.graphalytics.util.graph.PropertyGraph;

/**
 * Reference implementation of community detection algorithm.
 *
 * @author Stijn Heldens
 */
public class CommunityDetectionLPJob {
	private static final Logger LOG = LogManager.getLogger();

	private final PropertyGraph<Void, Void> graph;
	private final CommunityDetectionLPParameters parameters;

	public CommunityDetectionLPJob(PropertyGraph<Void, Void> graph, CommunityDetectionLPParameters parameters) {
		this.graph = graph;
		this.parameters = parameters;
	}

	public Long2LongMap run() {
		LOG.debug("- Starting community detection algorithm");

		// Read parameters
		int numVertices = graph.getVertices().size();
		int numIterations = parameters.getMaxIterations();

		// Initialize values
		Long2LongMap labels = new Long2LongOpenHashMap(numVertices);
		Long2LongMap newLabels = new Long2LongOpenHashMap(numVertices);
		Long2IntMap histogram = new Long2IntOpenHashMap();
		histogram.defaultReturnValue(0);

		// Set initial labels
		for (PropertyGraph<Void, Void>.Vertex v: graph.getVertices()) {
			labels.put(v.getId(), v.getId());
		}

		// Run iterations
		for (int it = 0; it < numIterations; it++) {
			LOG.debug("- Iteration " +  it);

			boolean change = false;

			for (PropertyGraph<Void, Void>.Vertex v: graph.getVertices()) {
				histogram.clear();

				// Count frequency of each label
				for (PropertyGraph<Void, Void>.Edge edge: v.getOutgoingEdges()) {
					long neighbor = edge.getDestinationVertex().getId();
					long label = labels.get(neighbor);
					histogram.put(label, histogram.get(label) + 1);
				}

				for (PropertyGraph<Void, Void>.Edge edge: v.getIncomingEdges()) {
					long neighbor = edge.getSourceVertex().getId();
					long label = labels.get(neighbor);
					histogram.put(label, histogram.get(label) + 1);
				}

				long bestLabel = 0;
				int bestCount = 0;

				// Select label with highest frequency. In case of a tie,
				// the label with the lowest value is chosen.
				for (long label: histogram.keySet()) {
					int count = histogram.get(label);

					if (count > bestCount || (count == bestCount && label < bestLabel)) {
						bestLabel = label;
						bestCount = count;
					}
				}

				// Set new label and check if label of vertex has changed
				newLabels.put(v.getId(), bestLabel);
				change = change || labels.get(v.getId()) != bestLabel;
			}

			Long2LongMap tmp = labels;
			labels = newLabels;
			newLabels = tmp;

			if (!change) {
				break;
			}
		}

		LOG.debug("- Finished community detection algorithm");

		return labels;
	}
}
