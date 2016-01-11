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

import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.Benchmark;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.NestedConfiguration;
import nl.tudelft.graphalytics.domain.PlatformBenchmarkResult;

/**
 * Sequential Java implementation of the Graphalytics benchmark.
 *
 * @author Tim Hegeman
 */
public class SequentialJavaPlatform implements Platform {

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		// TODO
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {
		// TODO
		return null;
	}

	@Override
	public void deleteGraph(String graphName) {
		// TODO
	}

	@Override
	public String getName() {
		return "sequentialjava";
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		return NestedConfiguration.empty();
	}

}
