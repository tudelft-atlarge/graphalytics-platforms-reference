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
package nl.tudelft.graphalytics.reference.algorithms.cd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongList;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionParameters;
import nl.tudelft.graphalytics.reference.GraphParser;

/**
 * Reference implementation of community detection algorithm.
 *
 * @author Stijn Heldens
 */
public class CommunityDetectionJob {
	private static final Logger LOG = LogManager.getLogger();

	private final Long2ObjectMap<LongList> graphData;
	private final CommunityDetectionParameters parameters;

	public CommunityDetectionJob(Long2ObjectMap<LongList> graphData, CommunityDetectionParameters parameters) {
		this.graphData = GraphParser.convertToUndirected(graphData);
		this.parameters = parameters;
	}

	public Long2LongMap run() {
		LOG.debug("- Starting community detection algorithm");
		
		// Read parameters
		int numVertices = graphData.size();
		int numIterations = parameters.getMaxIterations();
		
		// Initialize values
		Long2LongMap labels = new Long2LongOpenHashMap(numVertices);
		Long2LongMap newLabels = new Long2LongOpenHashMap(numVertices);
		Long2IntMap histogram = new Long2IntOpenHashMap();
		histogram.defaultReturnValue(0);
		
		// Set initial labels
		for (long v: graphData.keySet()) {
			labels.put(v, v);
		}
		
		// Run iterations
		for (int it = 0; it < numIterations; it++) {
			LOG.debug("- Iteration " +  it);
			
			boolean change = false;
			
			for (long v: graphData.keySet()) {
				histogram.clear();
				
				// Count frequency of each label
				for (long neighbor: graphData.get(v)) {
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
				newLabels.put(v, bestLabel);
				change |= labels.get(v) != bestLabel;
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
