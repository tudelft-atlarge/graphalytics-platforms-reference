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
package nl.tudelft.graphalytics.reference.algorithms.wcc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.AbstractLongPriorityQueue;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongList;
import nl.tudelft.graphalytics.reference.GraphParser;

/**
 * Reference implementation of connected components algorithm.
 *
 * @author Stijn Heldens
 */
public class WeaklyConnectedComponentsJob {
	private static final Logger LOG = LogManager.getLogger();

	private final Long2ObjectMap<LongList> graphData;

	public WeaklyConnectedComponentsJob(Long2ObjectMap<LongList> graphData, boolean directed) {
		this.graphData = directed ? GraphParser.convertToUndirected(graphData) : graphData;
	}
	
	public Long2LongMap run() {
		LOG.debug("- Starting connected components algorithm");

		Long2LongMap vertex2component = new Long2LongOpenHashMap(graphData.size());
		long numComponents = 0;
		
		for (long v: graphData.keySet()) {
			// skip vertex if already assigned to component
			if (vertex2component.containsKey(v)) {
				continue;
			}
			
			// Assign to new component
			long componentId = numComponents++;
			vertex2component.put(v, componentId);
			
			// Perform BFS starting at v to find members of component
			AbstractLongPriorityQueue queue = new LongArrayFIFOQueue();
			queue.enqueue(v);

			while (!queue.isEmpty()) {
				long u = queue.dequeueLong();
				
				for (long neighbour: graphData.get(u)) {
					if (!vertex2component.containsKey(neighbour)) {
						vertex2component.put(neighbour, componentId);
						queue.enqueue(neighbour);
					}
				}
			}
			
		}
		
		LOG.debug("- Finished connected components");
		
		return vertex2component;
	}
}
