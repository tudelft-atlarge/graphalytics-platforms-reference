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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import nl.tudelft.graphalytics.BenchmarkMetric;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.Algorithm;
import nl.tudelft.graphalytics.domain.Benchmark;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.NestedConfiguration;
import nl.tudelft.graphalytics.domain.PlatformBenchmarkResult;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import nl.tudelft.graphalytics.domain.graph.PropertyList;
import nl.tudelft.graphalytics.domain.graph.PropertyType;
import nl.tudelft.graphalytics.reference.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.reference.algorithms.cdlp.CommunityDetectionLPJob;
import nl.tudelft.graphalytics.reference.algorithms.lcc.LocalClusteringCoefficientJob;
import nl.tudelft.graphalytics.reference.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.reference.algorithms.sssp.SingleSourceShortestPathJob;
import nl.tudelft.graphalytics.reference.algorithms.wcc.WeaklyConnectedComponentsJob;
import nl.tudelft.graphalytics.util.graph.PropertyGraph;
import nl.tudelft.graphalytics.util.graph.PropertyGraphParser;
import nl.tudelft.graphalytics.util.graph.PropertyGraphParser.ValueParser;

/**
 * Reference implementation of the Graphalytics benchmark.
 *
 * @author Tim Hegeman
 */
public class ReferencePlatform implements Platform {

	private PropertyGraph graph;

	private class VoidParser implements ValueParser<Void> {
		@Override
		public Void parse(String[] tokens) throws IOException {
			return null;
		}
	}

	private class DoubleParser implements ValueParser<Double> {
		@Override
		public Double parse(String[] tokens) throws IOException {
			return Double.parseDouble(tokens[0]);
		}
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void uploadGraph(Graph graph) throws Exception {
		ValueParser vertexParser = getValueParser(graph.getVertexProperties());
		ValueParser edgeParser = getValueParser(graph.getEdgeProperties());

		this.graph = PropertyGraphParser.parsePropertyGraph(
				graph.getVertexFilePath(),
				graph.getEdgeFilePath(),
				graph.isDirected(),
				vertexParser,
				edgeParser);
	}

	private PropertyGraph convertToPropertyGraph(Graph graph) throws Exception {
		ValueParser vertexParser = getValueParser(graph.getVertexProperties());
		ValueParser edgeParser = getValueParser(graph.getEdgeProperties());

		return PropertyGraphParser.parsePropertyGraph(
				graph.getVertexFilePath(),
				graph.getEdgeFilePath(),
				graph.isDirected(),
				vertexParser,
				edgeParser);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {
		Algorithm algorithm = benchmark.getAlgorithm();
		Object parameters = benchmark.getAlgorithmParameters();
		Map<Long, ? extends Object> output;


		PropertyGraph graph = null;
		try {
			graph = convertToPropertyGraph(benchmark.getGraph());
		} catch (Exception e) {
			e.printStackTrace();
		}

		switch (algorithm) {
			case BFS:
				output = new BreadthFirstSearchJob((PropertyGraph<Void, Void>) graph, (BreadthFirstSearchParameters)parameters).run();
				break;
			case CDLP:
				output = new CommunityDetectionLPJob((PropertyGraph<Void, Void>) graph, (CommunityDetectionLPParameters)parameters).run();
				break;
			case WCC:
				output = new WeaklyConnectedComponentsJob(graph).run();
				break;
			case PR:
				output = new PageRankJob((PropertyGraph<Void, Void>) graph, (PageRankParameters)parameters).run();
				break;
			case LCC:
				output = new LocalClusteringCoefficientJob(graph).run();
				break;
			case SSSP:
				output = new SingleSourceShortestPathJob((PropertyGraph<Void, Double>) graph, (SingleSourceShortestPathsParameters)parameters).run();
				break;
			default:
				throw new PlatformExecutionException("Unsupported algorithm: " + algorithm);
		}

		if (benchmark.isOutputRequired()) {
			try {
				writeOutput(benchmark.getOutputPath(), output);
			} catch(IOException e) {
				throw new PlatformExecutionException("An error while writing to output file", e);
			}
		}

		return new PlatformBenchmarkResult(getPlatformConfiguration(), true);
	}

	@Override
	public void deleteGraph(String graphName) {
		graph = null;
	}

	@Override
	public BenchmarkMetric extractMetric() {
		return null; //TODO implement this;
	}

	private ValueParser getValueParser(PropertyList props) {
		if (props.size() == 0) {
			return new VoidParser();
		} else if (props.size() == 1 && props.get(0).getType().equals(PropertyType.REAL)) {
			return new DoubleParser();
		} else {
			throw new IllegalArgumentException("failed to find property value parser for properties: " + props);
		}
	}

	private void writeOutput(String path, Map<Long, ? extends Object> output) throws IOException {
		try (PrintWriter w = new PrintWriter(new FileOutputStream(path))) {
			for (Map.Entry<Long, ? extends Object> entry: output.entrySet()) {
				w.print(entry.getKey());
				w.print(" ");
				w.print(entry.getValue());
				w.println();
			}
		}
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
