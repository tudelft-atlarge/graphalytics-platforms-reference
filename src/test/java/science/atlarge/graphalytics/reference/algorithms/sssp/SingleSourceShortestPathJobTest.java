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
package science.atlarge.graphalytics.reference.algorithms.sssp;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import science.atlarge.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import science.atlarge.graphalytics.util.graph.PropertyGraph;
import science.atlarge.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsOutput;
import science.atlarge.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsValidationTest;

/**
 * Validation tests for the reference BFS implementation.
 *
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathJobTest extends SingleSourceShortestPathsValidationTest {


	@Override
	public SingleSourceShortestPathsOutput executeDirectedSingleSourceShortestPaths(
			PropertyGraph<Void, Double> graph,
			SingleSourceShortestPathsParameters parameters) throws Exception {
		return execute(graph, parameters);
	}

	@Override
	public SingleSourceShortestPathsOutput executeUndirectedSingleSourceShortestPaths(
			PropertyGraph<Void, Double> graph,
			SingleSourceShortestPathsParameters parameters) throws Exception {
		return execute(graph, parameters);
	}

	private SingleSourceShortestPathsOutput execute(PropertyGraph<Void, Double> graph,
			SingleSourceShortestPathsParameters parameters) throws Exception {
		Long2DoubleMap output = new SingleSourceShortestPathJob(graph, parameters).run();
		return new SingleSourceShortestPathsOutput(output);
	}
}
