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
package science.atlarge.graphalytics.reference.algorithms.lcc;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import science.atlarge.graphalytics.util.graph.PropertyGraph;

/**
 * Reference implementation of local clustering coefficient calculation.
 *
 * @author Stijn Heldens
 */
public class LocalClusteringCoefficientJob {
	private static final Logger LOG = LogManager.getLogger();

	private final PropertyGraph<Void, Void> graph;

	public LocalClusteringCoefficientJob(PropertyGraph<Void, Void> graph) {
		this.graph = graph;
	}

	public Long2DoubleMap run() {
		LOG.debug("- Starting local clustering coefficient calculation");

		Long2DoubleMap lcc = new Long2DoubleOpenHashMap();

		for (PropertyGraph<Void, Void>.Vertex v: graph.getVertices()) {
			int tri = 0;
			Set<PropertyGraph<Void, Void>.Vertex> v_neighbours = new HashSet<>();

			for (PropertyGraph<Void, Void>.Edge e: v.getIncomingEdges()) {
				v_neighbours.add(e.getSourceVertex());
			}

			for (PropertyGraph<Void, Void>.Edge e: v.getOutgoingEdges()) {
				v_neighbours.add(e.getDestinationVertex());
			}

			for (PropertyGraph<Void, Void>.Vertex u: v_neighbours) {
				for (PropertyGraph<Void, Void>.Edge e: u.getOutgoingEdges()) {
					if (v_neighbours.contains(e.getDestinationVertex())) {
						tri++;
					}
				}
			}

			int degree = v_neighbours.size();

			double result = degree >= 2 ? tri / (degree * (degree - 1.0)) : 0.0;
			lcc.put(v.getId(), result);
		}

		LOG.debug("- Finished local clustering coefficient calculation");

		return lcc;
	}
}
