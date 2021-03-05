package helloworld.apache.beam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class WordCountApacheBeam {

	public static void main(String... args) {

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline pipeline = Pipeline.create(options);

		// read from text file
		PCollection<String> lines = pipeline.apply("read from file", TextIO.read().from("input_file1.txt"));
		// split each line into words
		PCollection<List<String>> wordsPerLine = lines
				.apply(MapElements.via(new SimpleFunction<String, List<String>>() {

					@Override
					public List<String> apply(String input) {

						List<String> list = new ArrayList<String>();
						list = Arrays.asList(input.split(" "));
						for (String string : list) {
							System.out.println(" Data " + string);
						}
						return list;
					}
				}));

		PCollection<String> words = wordsPerLine
				.apply(FlatMapElements.via(new SimpleFunction<List<String>, Iterable<String>>() {

					@Override
					public Iterable<String> apply(List<String> input) {

						Iterable<String> iterable = input;
						for (String s : iterable) {
							System.out.println(" Flat Data ::: " + s);
						}
						return iterable;
					}

				}));

		//PCollection<KV<String, Long>> wordCount = words.apply(Count.perElement());
		
		PCollection<KV<String, Long>> wordCount = words.apply(MapElements.via(new SimpleFunction<String, KV<String, Long>>() {
			@Override
			public KV<String, Long> apply(String input) {
				System.out.println(" count data" + input);
				return KV.of(input, new Long("1"));
			}
		}))
		.apply(Count.perKey());
		//.apply(Sum.longsPerKey());

		// count each word occurred
		wordCount.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
			@Override
			public String apply(KV<String, Long> input) {

				return String.format("%s==%s", input.getKey(), input.getValue());
			}
		})).apply(TextIO.write().to("word-count"));

		pipeline.run().waitUntilFinish();
	}

}
