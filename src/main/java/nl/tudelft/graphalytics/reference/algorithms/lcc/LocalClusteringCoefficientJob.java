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
package nl.tudelft.graphalytics.reference.algorithms.lcc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import nl.tudelft.graphalytics.reference.GraphParser;

/**
 * Reference implementation of local clustering coefficient calculation.
 *
 * @author Stijn Heldens
 */
public class LocalClusteringCoefficientJob {
	private static final Logger LOG = LogManager.getLogger();

	private final GraphParser graph;

	public LocalClusteringCoefficientJob(GraphParser graph) {
		this.graph = graph;
	}

	public Long2DoubleMap run() {
		LOG.debug("- Starting local clustering coefficient calculation");

		Long2ObjectMap<LongSet> outNeighbors = new Long2ObjectOpenHashMap<>();
		Long2ObjectMap<LongSet> neighbors = new Long2ObjectOpenHashMap<>();

		for (long v: graph.getVertices()) {
			outNeighbors.put(v, new LongOpenHashSet());
			neighbors.put(v, new LongOpenHashSet());
		}

		for (long v: graph.getVertices()) {
			for (long u: graph.getNeighbors(v)) {
				outNeighbors.get(v).add(u);
				neighbors.get(u).add(v);
				neighbors.get(v).add(u);
			}
		}

		Long2DoubleMap lcc = new Long2DoubleOpenHashMap();

		for (long v: graph.getVertices()) {
			int tri = 0;
			LongSet v_neighbours = neighbors.get(v);

			for (long u: v_neighbours) {
				LongSet u_neighbours = outNeighbors.get(u);

				for (long neighbour: v_neighbours) {
					if (u_neighbours.contains(neighbour)) {
						tri++;
					}
				}
			}

			int degree = v_neighbours.size();

			double result = degree >= 2 ? tri / (degree * (degree - 1.0)) : 0.0;
			lcc.put(v, result);
		}

		LOG.debug("- Finished local clustering coefficient calculation");

		return lcc;
	}
}
