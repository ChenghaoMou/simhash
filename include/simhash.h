//
// Created by Chenghao Mou on 1/22/22.
//

#ifndef SIMHASH_SIMHASH_H
#define SIMHASH_SIMHASH_H

#include <cstddef>
#include <mutex>
#include <stdint.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace Simhash {

/**
 * The type of all hashes.
 */
typedef uint64_t hash_t;

/**
 * The type of a match of two hashes.
 */
typedef std::pair<hash_t, hash_t> match_t;

/**
 * For use with matches_t.
 */
struct match_t_hash {
  inline std::size_t operator()(const std::pair<hash_t, hash_t> &v) const {
    return static_cast<hash_t>(v.first * 31 + v.second);
  }
};

/**
 * The type for matches what we've returned.
 */
typedef std::unordered_set<match_t, match_t_hash> matches_t;

/**
 * The type of a set of clusters.
 */
typedef std::unordered_set<hash_t> cluster_t;
typedef std::vector<cluster_t> clusters_t;

/**
 * The number of bits in a hash_t.
 */
static const size_t BITS = sizeof(hash_t) * 8;

/**
 * Compute the number of bits that are flipped between two numbers
 *
 * @param a - reference number
 * @param b - number to compare
 *
 * @return number of bits that differ between a and b */
size_t num_differing_bits(hash_t a, hash_t b);

/**
 * Compute the simhash of a vector of hashes.
 */
hash_t compute(const std::vector<hash_t> &hashes);

/**
 * Find the set of all matches within the provided vector of hashes.
 *
 * The provided hashes are manipulated in place, but upon completion are
 * restored to their original state.
 */
matches_t find_all(std::unordered_set<hash_t> &hashes, size_t number_of_blocks,
                   size_t different_bits);

/**
 * Find all the clusters of simhashes.
 *
 * For a simhash to be added to a cluster, there must be a member in the
 * cluster already that is within `number_of_blocks` of the hash.
 */
clusters_t find_clusters(std::unordered_set<hash_t> &hashes,
                         size_t number_of_blocks, size_t different_bits);

class Permutation {
public:
  /**
   * Create a vector of permutations necessary to do all simhash near-dup
   * detection.
   */
  static std::vector<Permutation> create(size_t number_of_blocks,
                                         size_t different_bits);

  /**
   * Generate combinations of length r from population.
   */
  static std::vector<std::vector<hash_t>>
  choose(const std::vector<hash_t> &population, size_t r);

  /**
   * Construct a permutation from its permutation masks and the maximum
   * number of bits that may differ.
   */
  Permutation(size_t different_bits, std::vector<hash_t> &masks);

  /**
   * Apply this permutation.
   */
  hash_t apply(hash_t hash) const;

  /**
   * Reverse this permutation, getting the original.
   */
  hash_t reverse(hash_t hash) const;

  /**
   * Search mask
   *
   * When searching for a potential match, a match may be less than
   * the query, so it's insufficient to simply search for the query.
   * This mask sets all the bits in the last _differing_bits_ blocks
   * to 0, which is smaller than is necessary, but the easiest correct
   * number to compute. The highest number which must be potentially
   * searched is the query with all the bits in the last
   * _differing_bits_ blocks set to 1. */
  hash_t search_mask() const;

private:
  std::vector<hash_t> forward_masks;
  std::vector<hash_t> reverse_masks;
  std::vector<int> offsets;
  hash_t search_mask_;
};

//    void process_permutation(std::unordered_set<hash_t>const& hashes,
//    Simhash::Permutation const& permutation, size_t different_bits,
//    Simhash::matches_t& results, std::vector<Simhash::hash_t>& copy);

} // namespace Simhash

#endif // SIMHASH_SIMHASH_H
