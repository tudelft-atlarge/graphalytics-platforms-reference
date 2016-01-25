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

import it.unimi.dsi.fastutil.longs.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import nl.tudelft.graphalytics.reference.GraphParser;

/**
 * Reference implementation of local clustering coefficient calculation.
 *
 * @author Stijn Heldens
 */
public class LocalClusteringCoefficientJob {
	private static final Logger LOG = LogManager.getLogger();

	private final Long2ObjectMap<LongSet> outgoingEdgeData;
	private final Long2ObjectMap<LongSet> neighbourhoodData;

	public LocalClusteringCoefficientJob(Long2ObjectMap<LongList> graphData, boolean directed) {
		this.outgoingEdgeData = removeDuplicateNeighbours(graphData);
		this.neighbourhoodData = directed ?
				removeDuplicateNeighbours(GraphParser.convertToUndirected(graphData)) :
				outgoingEdgeData;
	}

	public Long2DoubleMap run() {
		LOG.debug("- Starting local clustering coefficient calculation");
		
		Long2DoubleMap lcc = new Long2DoubleOpenHashMap(outgoingEdgeData.size());
		
		for (long v: outgoingEdgeData.keySet()) {
			int tri = 0;
			LongSet v_neighbours = neighbourhoodData.get(v);
			
			for (long u: v_neighbours) {
				LongSet u_neighbours = outgoingEdgeData.get(u);

				for (long neighbour : v_neighbours) {
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
	
	static private Long2ObjectMap<LongSet> removeDuplicateNeighbours(Long2ObjectMap<LongList> graphData) {
		Long2ObjectMap<LongSet> uniqueNeighbours = new Long2ObjectOpenHashMap<>(graphData.size());
		for (long v: graphData.keySet()) {
			LongList neighbours = graphData.get(v);
			LongSet uniqueNeighboursForVertex = new LongOpenHashSet(neighbours);
			uniqueNeighbours.put(v, uniqueNeighboursForVertex);
		}
		return uniqueNeighbours;
	}
}
