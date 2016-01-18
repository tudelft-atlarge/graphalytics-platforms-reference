package nl.tudelft.graphalytics.reference.algorithms.stats;

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
		this.outgoingEdgeData = removeDuplicateNeighbors(graphData);
		this.neighbourhoodData = directed ?
				removeDuplicateNeighbors(GraphParser.convertToUndirected(graphData)) :
				outgoingEdgeData;
	}

	public Long2DoubleMap run() {
		LOG.debug("- Starting local clustering coefficient calculation");
		
		Long2DoubleMap lcc = new Long2DoubleOpenHashMap(outgoingEdgeData.size());
		
		for (long v: outgoingEdgeData.keySet()) {
			int tri = 0;
			LongSet v_neighbors = neighbourhoodData.get(v);
			
			for (long u: v_neighbors) {
				LongSet u_neighbors = outgoingEdgeData.get(u);

				for (long neighbour : v_neighbors) {
					if (u_neighbors.contains(neighbour)) {
						tri++;
					}
				}
			}
			
			int degree = v_neighbors.size();

			double result = degree >= 2 ? tri / (degree * (degree - 1.0)) : 0.0;
			lcc.put(v, result);
		}
		
		LOG.debug("- Finished local clustering coefficient calculation");
		
		return lcc;
	}
	
	static private Long2ObjectMap<LongSet> removeDuplicateNeighbors(Long2ObjectMap<LongList> graphData) {
		Long2ObjectMap<LongSet> uniqueNeighbours = new Long2ObjectOpenHashMap<>(graphData.size());
		for (long v: graphData.keySet()) {
			LongList neighbors = graphData.get(v);
			LongSet uniqueNeighboursForVertex = new LongOpenHashSet(neighbors);
			uniqueNeighbours.put(v, uniqueNeighboursForVertex);
		}
		return uniqueNeighbours;
	}
}
