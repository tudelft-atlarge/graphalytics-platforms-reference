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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.reference.Util;
import nl.tudelft.graphalytics.util.graph.PropertyGraph;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsOutput;
import nl.tudelft.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsValidationTest;

/**
 * Validation tests for the reference connected components implementation.
 *
 * @author Stijn Heldens
 */
public class WeaklyConnectedComponentsJobTest extends WeaklyConnectedComponentsValidationTest {

	@Override
	public WeaklyConnectedComponentsOutput executeDirectedConnectedComponents(GraphStructure graph) throws Exception {
		return execute(graph, true);
	}

	@Override
	public WeaklyConnectedComponentsOutput executeUndirectedConnectedComponents(GraphStructure graph) throws Exception {
		return execute(graph, false);
	}

	private WeaklyConnectedComponentsOutput execute(GraphStructure graph, boolean directed) throws Exception {
		PropertyGraph<Void, Void> pgraph = Util.convertToPropertyGraph(graph);
		Long2LongMap output = new WeaklyConnectedComponentsJob(pgraph).run();
		return new WeaklyConnectedComponentsOutput(output);
	}
}
