package nl.tudelft.graphalytics.reference.algorithms.conn;

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
public class ConnectedComponentsJob {
	private static final Logger LOG = LogManager.getLogger();

	private final Long2ObjectMap<LongList> graphData;

	public ConnectedComponentsJob(Long2ObjectMap<LongList> graphData, boolean directed) {
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
