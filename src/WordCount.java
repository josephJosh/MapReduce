//*************************************************************************//
// Josh Joseph

// Class: WordCount

// Java implementation of MapReduce using 3 Mappers and 2 Reducers
//*************************************************************************//

// Import statements
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class WordCount {

	// Driver for MapReduce
	public static void main(String[] args) throws Exception {

		// Reading three data files and saving each one of them as a String
		String map1 = readFile("src/input/data1.txt");
		String map2 = readFile("src/input/data2.txt");
		String map3 = readFile("src/input/data3.txt");

		WordCount wc = new WordCount();

		wc.mapper(map1); // Mapper 1

		WordCount wc1 = new WordCount();

		wc1.mapper(map2); // Mapper 2
		wc1.mapper(map3); // Mapper 3

		wc.reducer(); // Reducer 1
		wc1.reducer(); // Reducer 1
	}

	// Method to read a text file and save the contents as a String
	public static String readFile(String fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			String text = sb.toString();
			String textEdited = beautify(text);

			return textEdited;
		} finally {
			br.close();
		}
	}

	// Method to alter the String to count the words better
	public static String beautify(String text) {
		text = text.toLowerCase();
		text = text.replaceAll("\\.", "");
		text = text.replaceAll("\'", "");
		return text;
	}

	// Method to sort the key value pairs (word, count) using TreeMap
	public Map<String, Integer> sort(HashMap<String, Integer> rawData) {
		Map<String, Integer> map = new TreeMap<String, Integer>(rawData);
		return map;
	}

	// Method which implements Mapper (Intermediate Step)
	// Add number "1" to each word from the text file sequentially
	public void mapper(String text) throws IOException {
		String[] words = text.split(" ");
		File f = new File("src/input/inter1.txt");
		if (f.exists() && !f.isDirectory()) {
			// Mapper 2 and Mapper 3
			BufferedWriter writer1 = new BufferedWriter(new FileWriter(new File("src/input/inter2.txt"), true));
			int one = 1;
			for (String val : words)
				writer1.append(val + "," + one + "\n"); // The number "1" is
														// added next to each
														// word
			writer1.newLine();
			writer1.newLine();

			writer1.close();
		} else {
			// Mapper 1
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File("src/input/inter1.txt")));
			int one = 1;
			for (String val : words)
				writer.append(val + "," + one + "\n"); // The number "1" is
														// added next to each
														// word
			writer.close();
		}
	}

	// Method which implements Reducer - uses the Mapper to find the WordCount
	// for each word
	public void reducer() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(new File("src/input/inter1.txt")));
		BufferedReader reader1 = new BufferedReader(new FileReader(new File("src/input/inter2.txt")));

		String line;
		int one = 1;
		HashMap<String, Integer> result = new HashMap<String, Integer>();
		while ((line = reader.readLine()) != null) {
			String[] words = line.split(","); // split the string at comma (,)
			if (result.containsKey(words[0]))
				result.put(words[0], result.get(words[0]) + one); // increments
																	// 1 each
																	// time the
																	// same word
																	// is found
			else
				result.put(words[0], one);
		}

		Map<String, Integer> map = sort(result); // sort the results for reducer
													// 1

		reader.close();

		// Printing Reducer 1
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File("src/output/out1.txt")));
		for (String key : map.keySet()) {
			writer.append("<" + key + ", " + map.get(key) + "> " + "\n");
		}
		writer.close();

		result = new HashMap<String, Integer>();
		while ((line = reader1.readLine()) != null) {
			String[] words = line.split(","); // split the string at comma (,)
			if (result.containsKey(words[0]))
				result.put(words[0], result.get(words[0]) + one); // increments
																	// 1 each
																	// time the
																	// same word
																	// is found
			else
				result.put(words[0], one);
		}

		map = sort(result); // sort the results for reducer 2

		reader1.close();

		// Printing Reducer 2
		BufferedWriter writer1 = new BufferedWriter(new FileWriter(new File("src/output/out2.txt")));
		for (String key : map.keySet()) {
			writer1.append("<" + key + ", " + map.get(key) + "> " + "\n");
		}

		writer1.close();

		// Formatting and printing both reducers in one file
		BufferedWriter writer2 = new BufferedWriter(new FileWriter(new File("src/output/FinalOutput.txt")));
		String file1 = readFile("src/output/out1.txt");
		String file2 = readFile("src/output/out2.txt");

		writer2.append("Reducer 1:");
		writer2.newLine();

		writer2.append(file1);

		writer2.newLine();
		writer2.newLine();
		writer2.newLine();
		writer2.append("Reducer 2:");
		writer2.newLine();
		writer2.append(file2);

		writer2.close();
	}
}