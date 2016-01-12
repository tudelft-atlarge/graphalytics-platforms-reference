package nl.tudelft.graphalytics.reference.algorithms.stats;

import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
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

	private final Long2ObjectMap<LongList> undirectedGraphData;

	public LocalClusteringCoefficientJob(Long2ObjectMap<LongList> graphData, boolean directed) {
		this.undirectedGraphData = directed ? GraphParser.convertToUndirected(graphData) : graphData;
		removeDuplicateNeighbors(this.undirectedGraphData);
	}

	public Long2DoubleMap run() {
		LOG.debug("- Starting local clustering coefficient calculation");
		
		Long2DoubleMap lcc = new Long2DoubleOpenHashMap(undirectedGraphData.size());
		double sum = 0;
		
		for (long v: undirectedGraphData.keySet()) {
			int tri = 0;
			
			for (long u: undirectedGraphData.get(v)) {
				LongList v_neighbors = undirectedGraphData.get(u);
				LongList u_neighbors = undirectedGraphData.get(v);
				
				int i = 0;
				int j = 0;
				
				while (i < v_neighbors.size() && j < u_neighbors.size()) {
					long delta = v_neighbors.getLong(i) - u_neighbors.getLong(j);
					
					if (delta == 0) tri++;
					if (delta <= 0) i++;
					if (delta >= 0) j++;
				}
			}
			
			int degree = undirectedGraphData.get(v).size();
			LOG.debug(v + " " + tri +  " " + degree);
			double result = degree >= 2 ? tri / (degree * (degree - 1.0)) : 0.0;
			lcc.put(v, result);
		}
		
		LOG.debug("- Finished local clustering coefficient calculation");
		
		return lcc;
	}
	
	static private void  removeDuplicateNeighbors(Long2ObjectMap<LongList> graphData) {
		for (long v: graphData.keySet()) {
			LongList neighbors = graphData.get(v);
			Collections.sort(neighbors);
			
			long prev = -1;
			int index = 0;
			int size = neighbors.size();
			
			for (int i = 0; i < size; i++) {
				if (neighbors.getLong(i) != prev) {
					neighbors.set(index++, prev = neighbors.getLong(i));
				}
			}
			
			neighbors.size(index);
		}
	}
}
