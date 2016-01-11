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
package nl.tudelft.graphalytics.sequentialjava;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import nl.tudelft.graphalytics.validation.GraphStructure;

/**
 * Parser for the validation graph structure used by the Graphalytics validation framework.
 *
 * @author Tim Hegeman
 */
public final class ValidationGraphParser {

	public static Long2ObjectMap<LongList> parseValidationGraph(GraphStructure graphStructure) {
		Long2ObjectMap<LongList> parsedGraph = new Long2ObjectOpenHashMap<>(graphStructure.getVertices().size());
		for (long vertex : graphStructure.getVertices()) {
			parsedGraph.put(vertex, new LongArrayList(graphStructure.getEdgesForVertex(vertex).size()));
			for (long neighbour : graphStructure.getEdgesForVertex(vertex)) {
				parsedGraph.get(vertex).add(neighbour);
			}
		}
		return parsedGraph;
	}

}
