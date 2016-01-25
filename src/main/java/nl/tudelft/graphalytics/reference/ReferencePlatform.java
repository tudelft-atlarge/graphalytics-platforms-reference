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
package nl.tudelft.graphalytics.reference;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongList;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.*;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.reference.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.reference.algorithms.cdlp.CommunityDetectionLPJob;
import nl.tudelft.graphalytics.reference.algorithms.wcc.WeaklyConnectedComponentsJob;
import nl.tudelft.graphalytics.reference.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.reference.algorithms.lcc.LocalClusteringCoefficientJob;

/**
 * Reference implementation of the Graphalytics benchmark.
 *
 * @author Tim Hegeman
 */
public class ReferencePlatform implements Platform {

	private Long2ObjectMap<LongList> graphEdges;
	private boolean graphDirected;

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		graphEdges = new GraphParser(graph).parse();
		graphDirected = graph.getGraphFormat().isDirected();
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {
		Algorithm algorithm = benchmark.getAlgorithm();
		Object parameters = benchmark.getAlgorithmParameters();
		switch (algorithm) {
			case BFS:
				new BreadthFirstSearchJob(graphEdges, graphDirected, (BreadthFirstSearchParameters)parameters).run();
				break;
			case CDLP:
				new CommunityDetectionLPJob(graphEdges, (CommunityDetectionLPParameters)parameters).run();
				break;
			case WCC:
				new WeaklyConnectedComponentsJob(graphEdges, graphDirected).run();
				break;
			case PR:
				new PageRankJob(graphEdges, graphDirected, (PageRankParameters)parameters).run();
				break;
			case LCC:
				new LocalClusteringCoefficientJob(graphEdges, graphDirected).run();
				break;
			default:
				throw new PlatformExecutionException("Unsupported algorithm: " + algorithm);
		}
		return new PlatformBenchmarkResult(getPlatformConfiguration());
	}

	@Override
	public void deleteGraph(String graphName) {
		graphEdges = null;
	}

	@Override
	public String getName() {
		return "reference";
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		return NestedConfiguration.empty();
	}

}
