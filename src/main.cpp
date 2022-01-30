#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>
#include <unordered_set>

#include <getopt.h>

#include "../include/jenkins.h"
#include "../include/json.hh"
#include "../include/simhash.h"

void usage(int argc, char **argv)
{
  std::cout << "usage: " << argv[0] << " --blocks BLOCKS"
            << " --distance DISTANCE"
            << " --input INPUT"
            << " --format FORMAT"
            << " [--text_column=TEXT]"
            << " [--id_column=ID]"
            << " [--sample=SAMPLE]"
            << " [--window=WINDOW]"
            << " --output OUTPUT\n\n"
            << "Read simhashes or json lines from input, find all pairs within "
               "distance bits of \n"
            << "each other, writing them to output.\n\n"
            << "  --blocks BLOCKS        Number of bit blocks to use\n"
            << "  --distance DISTANCE    Maximum bit distances of matches\n"
            << "  --input INPUT          Path to input ('-' for stdin)\n"
            << "  --format               Format of the input, hash or json\n"
            << "  --text_column          Column of the text to hash, optional\n"
            << "  --id_column            Column of the index, optional\n"
            << "  --sample               Number of samples to take from the "
               "input, optional\n"
            << "  --window               Size of the hashing window, optional\n"
            << "  --output OUTPUT        Path to output ('-' for stdout)\n";
}

/*

1. Read hashes or json lines from the input and create hashes if it is json;
2. Store the hashes in the first argument `hashes`;
3. Store the hash-to-indices mapping in the second argument `hash2ids`;

If it is in json format:
1. Tokenize the text from the `text_column` with the `window` size;
2. Hash each token individually and compute the document fingerprint;

Only takes the first `sample` records if `sample` is larger than zero.
*/
void read_hashes(
    std::istream &stream, std::unordered_set<Simhash::hash_t> &hashes,
    std::map<Simhash::hash_t, std::unordered_set<std::string>> &hash2ids,
    std::string text_column, std::string id_column, std::string format,
    size_t sample, size_t window)
{
  Simhash::hash_t hash(0);
  Simhash::jenkins hasher;

  int count = 0;
  int limit = static_cast<int>(sample);
  int window_size = window > 0 ? static_cast<int>(window) : 5;

  std::string line;

  while (!stream.eof() && (limit > 0 ? (format == "hash" ? (count - 1 < limit) : (count < limit)) : true))
  {
    std::getline(stream, line);
    if (stream.fail())
    {
      break;
    }

    if (format == "hash")
    {
      // Skip the first line in the tsv file, assuming it is the header
      if (count++ == 0)
      {
        continue;
      }
      std::istringstream iss(line);
      std::string id, h;
      char *end;

      std::getline(iss, id, '\t');
      std::getline(iss, h, '\t');
      hash = strtoull(h.c_str(), &end, 10);

      hash2ids[hash].insert(id);
      hashes.insert(hash);
    }
    else if (format == "json")
    {
      auto j3 = nlohmann::json::parse(line);
      std::string text = j3[text_column];
      int index = j3[id_column];

      std::string id = std::to_string(index);
      std::vector<Simhash::hash_t> features;

      for (int i = 0; i < text.size() - window_size; i++)
      {
        std::string token = text.substr(i, window_size);
        const char *c = token.c_str();
        features.push_back(hasher.compute(c, token.size(), 0));
      }
      Simhash::hash_t hash = Simhash::compute(features);
      hash2ids[hash].insert(id);
      hashes.insert(hash);
      count++;
    }
  }
  std::cout << "Total " << count << " lines and " << hashes.size() << " hashes"
            << std::endl;
}

/* 
Write the clusters to a tsv file.
 */
void write_clusters(
    std::ostream &stream, const Simhash::clusters_t &clusters,
    std::map<Simhash::hash_t, std::unordered_set<std::string>> &hash2ids)
{
  std::cout << "Found " << clusters.size() << " clusters" << std::endl;
  stream << "id\thash\tcluster" << std::endl;

  int cluster_id = 0;
  for (const auto &cluster : clusters)
  {
    for (const auto &hash : cluster)
    {
      for (const auto &idx : hash2ids[hash])
      {
        stream << idx << "\t" << std::to_string(hash) << "\t" << cluster_id
               << std::endl;
      }
    }
    cluster_id++;
  }
  stream.flush();
}

int main(int argc, char **argv)
{

  unsigned int n = std::thread::hardware_concurrency();
  std::cout << n << " concurrent threads are supported.\n";

  auto start = std::chrono::high_resolution_clock::now();

  std::string input, output, text_column, id_column, format;
  size_t blocks(0), distance(0), sample(0), window(0);

  int getopt_return_value(0);
  while (getopt_return_value != -1)
  {
    int option_index = 0;
    static struct option long_options[] = {
        {"input", required_argument, 0, 0},
        {"text_column", optional_argument, 0, 0},
        {"id_column", optional_argument, 0, 0},
        {"output", required_argument, 0, 0},
        {"blocks", required_argument, 0, 0},
        {"distance", required_argument, 0, 0},
        {"help", no_argument, 0, 0},
        {"format", required_argument, 0, 0},
        {"sample", optional_argument, 0, 0},
        {"window", optional_argument, 0, 0},
        {0, 0, 0, 0}};

    getopt_return_value = getopt_long(argc, argv, "i:t::x::o:b:d:hf:n::w::", long_options, &option_index);

    switch (getopt_return_value)
    {
    case 0:
      switch (option_index)
      {
      case 0:
        input = optarg;
        break;
      case 1:
        text_column = optarg;
        break;
      case 2:
        id_column = optarg;
        break;
      case 3:
        output = optarg;
        break;
      case 4:
        std::stringstream(std::string(optarg)) >> blocks;
        break;
      case 5:
        std::stringstream(std::string(optarg)) >> distance;
        break;
      case 6:
        usage(argc, argv);
        return 0;
      case 7:
        format = optarg;
        break;
      case 8:
        std::stringstream(std::string(optarg)) >> sample;
        break;
      case 9:
        std::stringstream(std::string(optarg)) >> window;
        break;
      }
      break;
    case 'i':
      input = optarg;
      break;
    case 't':
      text_column = optarg;
      break;
    case 'x':
      id_column = optarg;
      break;
    case 'o':
      output = optarg;
      break;
    case 'b':
      std::stringstream(std::string(optarg)) >> blocks;
      break;
    case 'd':
      std::stringstream(std::string(optarg)) >> distance;
      break;
    case 'h':
      usage(argc, argv);
      return 0;
    case 'f':
      format = optarg;
      break;
    case 'n':
      std::stringstream(std::string(optarg)) >> sample;
      break;
    case 'w':
      std::stringstream(std::string(optarg)) >> window;
      break;
    case '?':
      return 1;
    }
  }

  if (blocks == 0)
  {
    std::cerr << "Blocks must be provided and > 0" << std::endl;
    return 2;
  }

  if (distance == 0)
  {
    std::cerr << "Distance must be provided and > 0" << std::endl;
    return 3;
  }

  if (input.empty())
  {
    std::cerr << "Input must be provided and non-empty." << std::endl;
    return 4;
  }

  if (output.empty())
  {
    std::cerr << "Output must be provided and non-empty." << std::endl;
    return 5;
  }

  if (blocks <= distance)
  {
    std::cerr << "Blocks (" << blocks << ") must be >= distance (" << distance
              << ")" << std::endl;
    return 6;
  }

  if (format.empty())
  {
    std::cerr << "Format must be provided (hash or json) and non-empty."
              << std::endl;
    return 7;
  }

  // Read the input
  std::unordered_set<Simhash::hash_t> hashes;
  std::map<Simhash::hash_t, std::unordered_set<std::string>> hash2ids;

  if (input.compare("-") == 0)
  {
    std::cerr << "Reading hashes from stdin." << std::endl;
    read_hashes(std::cin, hashes, hash2ids, text_column, id_column, format,
                sample, window);
  }
  else
  {
    std::cerr << "Reading hashes from " << input << std::endl;
    {
      std::ifstream fin(input, std::ifstream::in | std::ifstream::binary);
      if (!fin.good())
      {
        std::cerr << "Error reading " << input << std::endl;
        return 7;
      }
      read_hashes(fin, hashes, hash2ids, text_column, id_column, format, sample,
                  window);
    }
  }

  // Find matches
  std::cerr << "Computing matches..." << std::endl;
  Simhash::clusters_t clusters =
      Simhash::find_clusters(hashes, blocks, distance);

  // Write output
  if (output.compare("-") == 0)
  {
    std::cerr << "Writing results to stdout." << std::endl;
    write_clusters(std::cout, clusters, hash2ids);
  }
  else
  {
    std::cerr << "Writing matches to " << output << std::endl;
    {
      std::ofstream fout(output, std::ofstream::binary);
      if (!fout.good())
      {
        std::cerr << "Error writing " << output << std::endl;
        return 8;
      }
      write_clusters(fout, clusters, hash2ids);
    }
  }

  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cerr << "Total time: " << duration.count() / 1e6 << " seconds"
            << std::endl;
  return 0;
}