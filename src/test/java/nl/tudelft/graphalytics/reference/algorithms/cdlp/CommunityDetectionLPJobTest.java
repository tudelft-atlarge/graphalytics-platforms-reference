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
package nl.tudelft.graphalytics.reference.algorithms.cdlp;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.reference.Util;
import nl.tudelft.graphalytics.util.graph.PropertyGraph;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPOutput;
import nl.tudelft.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPValidationTest;

/**
 * Validation tests for the reference community detection implementation.
 *
 * @author Stijn Heldens
 */
public class CommunityDetectionLPJobTest extends CommunityDetectionLPValidationTest {

	@Override
	public CommunityDetectionLPOutput executeDirectedCommunityDetection(GraphStructure graph,
			CommunityDetectionLPParameters parameters) throws Exception {
		return execute(graph, parameters, true);
	}

	@Override
	public CommunityDetectionLPOutput executeUndirectedCommunityDetection(GraphStructure graph,
			CommunityDetectionLPParameters parameters) throws Exception {
		return execute(graph, parameters, false);
	}

	private CommunityDetectionLPOutput execute(GraphStructure graph, CommunityDetectionLPParameters parameters,
			boolean directed) throws Exception {
		PropertyGraph<Void, Void> pgraph = Util.convertToPropertyGraph(graph);
		Long2LongMap output = new CommunityDetectionLPJob(pgraph, parameters).run();
		return new CommunityDetectionLPOutput(output);
	}
}
