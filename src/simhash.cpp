#include "../include/simhash.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <list>
#include <sstream>
#include <thread>

// Calculate the hamming distance between two hash values
size_t Simhash::num_differing_bits(Simhash::hash_t a, Simhash::hash_t b)
{
  size_t count(0);
  Simhash::hash_t n = a ^ b;
  while (n)
  {
    ++count;
    n = n & (n - 1);
  }
  return count;
}

// Calculate the fingerprint based on the hash values
Simhash::hash_t Simhash::compute(const std::vector<Simhash::hash_t> &hashes)
{
  // Initialize counts to 0
  std::vector<long> counts(Simhash::BITS, 0);

  // Count the number of 1's, 0's in each position of the hashes
  for (auto it = hashes.begin(); it != hashes.end(); ++it)
  {
    Simhash::hash_t hash = *it;
    for (size_t i = 0; i < BITS; ++i)
    {
      counts[i] += (hash & 1) ? 1 : -1;
      hash >>= 1;
    }
  }

  // Produce the result
  Simhash::hash_t result(0);
  for (size_t i = 0; i < BITS; ++i)
  {
    if (counts[i] > 0)
    {
      result |= (static_cast<Simhash::hash_t>(1) << i);
    }
  }
  return result;
}

/**
 * Find all near-matches in a set of hashes.
 *
 * This works by putting the provided hashes into a vector. Then, for each
 * permutation, apply the permutation and sort the permuted hashes. Then walk
 * the hashes, finding each unique prefix.
 *
 * For each unique prefix, consider all hashes sharing that prefix, adding
 * matches with the lower number first (to avoid duplication; suppose a < b --
 * we will only emit (a, b) as a match, but (b, a) will not be emitted).
 */
Simhash::matches_t Simhash::find_all(
    std::unordered_set<Simhash::hash_t> &hashes,
    size_t number_of_blocks, size_t different_bits)
{
  std::vector<Simhash::hash_t> copy(hashes.begin(), hashes.end());
  Simhash::matches_t results;
  auto permutations =
      Simhash::Permutation::create(number_of_blocks, different_bits);
  std::vector<std::thread> threads;

  std::mutex mt;

  for (size_t i = 0; i < permutations.size(); i++)
  {
    Simhash::Permutation &permutation = permutations[i];
    // Apply the permutation to the set of hashes and sort
    auto op = [permutation](Simhash::hash_t h) -> Simhash::hash_t
    {
      return permutation.apply(h);
    };
    std::transform(hashes.begin(), hashes.end(), copy.begin(), op);
    std::sort(copy.begin(), copy.end());

    // Walk through and find regions that have the same prefix subject to the
    // mask
    Simhash::hash_t mask = permutation.search_mask();

    auto start = copy.begin();
    float progress = 0;
    int bar_width = 70;
    auto time_start = std::chrono::high_resolution_clock::now();
    while (start != copy.end())
    {
      std::cout << "[";
      // Find the end of the range that starts with this prefix
      Simhash::hash_t prefix = (*start) & mask;
      std::vector<Simhash::hash_t>::iterator end = start;
      for (; end != copy.end() && (*end & mask) == prefix; ++end, ++progress)
      {
      }

      // For all the hashes that are between start and end, consider them all

#pragma omp parallel for default(shared)
      for (auto a = start; a != end; ++a)
      {
        for (auto b = a + 1; b != end; ++b)
        {
          if (Simhash::num_differing_bits(*a, *b) <= different_bits)
          {
            Simhash::hash_t a_raw = permutation.reverse(*a);
            Simhash::hash_t b_raw = permutation.reverse(*b);
            // Insert the result keyed on the smaller of the two
            mt.lock();
            results.insert(
                std::make_pair(std::min(a_raw, b_raw), std::max(a_raw, b_raw)));
            mt.unlock();
          }
        }
      }
      auto time_end = std::chrono::high_resolution_clock::now();
      std::cout.flush();
      int pos = bar_width * progress / hashes.size();
      auto total = std::chrono::duration_cast<std::chrono::microseconds>(
          time_end - time_start);

      for (int j = 0; j < bar_width; ++j)
      {
        if (j < pos)
          std::cout << "=";
        else if (j == pos)
          std::cout << ">";
        else
          std::cout << " ";
      }
      std::cout << "] (" << std::setw(2) << i << ")";
      std::cout << std::right << std::setw(3)
                << int(progress / hashes.size() * 100.0) << "% "
                << std::setw(10) << int(total.count() / 1e6) << "/";
      std::cout << std::left
                << int(total.count() / progress * hashes.size() / 1e6)
                << " sec \r";
      std::cout.flush();

      // Advance start to after the block
      start = end;
    }
  }

  for (auto &th : threads)
  {
    th.join();
  }
  std::cout << "\n";

  return results;
}

Simhash::clusters_t Simhash::find_clusters(
    std::unordered_set<Simhash::hash_t> &hashes,
    size_t number_of_blocks, size_t different_bits)
{
  // Build up the edges of this graph
  std::unordered_map<Simhash::hash_t, std::unordered_set<Simhash::hash_t>>
      nodes;
  std::unordered_map<Simhash::hash_t, bool> visited;
  for (const auto &match : find_all(hashes, number_of_blocks, different_bits))
  {
    nodes[match.first].insert(match.second);
    nodes[match.second].insert(match.first);
    visited[match.first] = false;
    visited[match.second] = false;
  }

  // Go through every node that is connected to an edge, and conduct a BFS from
  // it to build a cluster. Skip nodes that have already been visited.
  Simhash::clusters_t clusters;
  for (const auto &node : nodes)
  {
    // If a node has already been visited, it's already in a cluster.
    if (visited[node.first])
    {
      continue;
    }

    // If a node has not been visited, then start a cluster emanating from it.
    visited[node.first] = true;
    Simhash::cluster_t cluster({node.first});
    std::list<Simhash::hash_t> frontier(node.second.begin(), node.second.end());
    while (!frontier.empty())
    {
      // The first frontier node is part of the cluster
      Simhash::hash_t neighbor = frontier.front();
      frontier.pop_front();
      cluster.insert(neighbor);
      visited[neighbor] = true;

      // Put every unvisited neighbor in the frontier
      for (Simhash::hash_t hash : nodes[neighbor])
      {
        if (!visited[hash])
        {
          frontier.push_back(hash);
          visited[hash] = true;
        }
      }
    }
    clusters.push_back(cluster);
  }

  return clusters;
}

std::vector<std::vector<Simhash::hash_t>> Simhash::Permutation::choose(const std::vector<hash_t> &population, size_t r)
{
  // This algorithm is cribbed from python's itertools page.
  size_t n = population.size();
  if (r > n)
  {
    throw std::invalid_argument("R cannot be greater than population size.");
  }

  std::vector<size_t> indices(r);
  for (size_t i = 0; i < r; ++i)
  {
    indices[i] = i;
  }

  std::vector<std::vector<hash_t>> results;
  std::vector<hash_t> result(r);

  for (size_t i = 0; i < r; ++i)
  {
    result[i] = population[indices[i]];
  }
  results.push_back(result);

  while (true)
  {
    int i = r - 1;
    for (; i >= 0; --i)
    {
      if (indices[i] != i + n - r)
      {
        break;
      }
    }
    if (i < 0)
    {
      return results;
    }

    indices[i] += 1;
    for (size_t j = i + 1; j < r; ++j)
    {
      indices[j] = indices[j - 1] + 1;
    }
    for (size_t j = 0; j < r; ++j)
    {
      result[j] = population[indices[j]];
    }
    results.push_back(result);
  }
}

std::vector<Simhash::Permutation> Simhash::Permutation::create(size_t number_of_blocks, size_t different_bits)
{
  if (number_of_blocks > Simhash::BITS)
  {
    std::stringstream message;
    message << "Number of blocks must not exceed " << sizeof(hash_t) * 8;
    throw std::invalid_argument(message.str());
  }

  if (number_of_blocks <= different_bits)
  {
    std::stringstream message;
    message << "Number of blocks (" << number_of_blocks
            << ") must be greater than different_bits (" << different_bits
            << ")";
    throw std::invalid_argument(message.str());
  }

  /* These are the blocks, in mask form. */
  std::vector<hash_t> blocks;
  for (size_t i = 0; i < number_of_blocks; ++i)
  {
    hash_t mask(0);
    size_t start = (i * Simhash::BITS) / number_of_blocks;
    size_t end = ((i + 1) * Simhash::BITS) / number_of_blocks;
    for (size_t j = start; j < end; ++j)
    {
      mask |= (static_cast<hash_t>(1) << j);
    }
    blocks.push_back(mask);
  }

  /* This is the number of blocks in the leading prefix. */
  size_t count = static_cast<size_t>(number_of_blocks - different_bits);

  /* All the mask choices. */
  std::vector<Permutation> results;
  for (std::vector<hash_t> &choice : choose(blocks, count))
  {
    // Add the remaining masks -- those that were not part of choice
    for (hash_t block : blocks)
    {
      if (find(choice.begin(), choice.end(), block) == choice.end())
      {
        choice.push_back(block);
      }
    }

    results.push_back(Permutation(different_bits, choice));
  }

  return results;
}

Simhash::Permutation::Permutation(size_t different_bits, std::vector<hash_t> &masks)
    : forward_masks(masks), reverse_masks(), offsets(), search_mask_(0)
{
  int j(0), i(0), width(0); // counters

  // All of these are O(forward_masks)
  std::vector<size_t> widths;
  widths.reserve(forward_masks.size());
  reverse_masks.reserve(forward_masks.size());
  offsets.reserve(forward_masks.size());

  std::vector<hash_t>::iterator mask_it(forward_masks.begin());

  /* To more easily and reasonably-efficiently calculate the permutations
   * of each of the hashes we insert, and since each block is just
   * contiguous set bits, we will calculate the widths of each of these
   * blocks, and the offset of their rightmost bit. With this, we'll
   * generate net offsets between their positions in the unpermuted and
   * permuted forms, and simultaneously generate reverse masks */
  for (; mask_it != forward_masks.end(); ++mask_it)
  {
    hash_t mask = *mask_it;
    /* Find where the 1's start, and where they end. After this, `i` is
     * the position to the right of the rightmost set bit. `j` is the
     * position of the leftmost set bit. In `width`, we keep a running
     * tab of the widths of the bitmasks so far. */
    for (i = 0; !((1UL << i) & mask); ++i)
    {
    }
    for (j = i; j < 64 && ((1UL << j) & mask); ++j)
    {
    }

    /* Just to prove that I'm sane, and in case that I'm ever running
     * circles around this logic in the future, consider:
     *
     *     63---53|52---42|41---32|31---21|20---10|09---00|
     *     |  A   |   B   |   C   |   D   |   E   |   F   |
     *
     *                       permuted to
     *     63---53|52---42|41---32|31---21|20---10|09---00|
     *     |  C   |   D   |   E   |   A   |   B   |   F   |
     *
     * The first loop, we'll have width = 0, and examining the mask for
     * C, we'll find that i = 31 and j = 41, so we find that its width
     * is 10. In the end, the bit in position 32 needs to move to be in
     * position 53. Width serves as an accumulator of the widths of the
     * blocks inserted into the permuted value, so we increment it by
     * the width of C (j-i) = 10. Now the end offset is 63 - width = 53,
     * and the original offset for that bit was (i+1) = 32, and we find
     * that we need an offset of 63 - width - i - 1 = 62 - width - i:
     *
     *    C: i = 31 | j = 41 | width = 0  | end = 53
     *       width += (j-i)          => 10
     *       offset = 62 - width - i => 21
     *
     *    D: i = 20 | j = 31 | width = 10 | end = 42
     *       width += (j-i)          => 21
     *       offset = 62 - width - i => 21 */
    width += (j - i);
    widths.push_back(j - i);

    int offset = 64 - width - i;
    offsets.push_back(offset);

    /* It's a trivial transformation, but we'll pre-compute our reverse
     * masks so that we don't have to compute for after the every time
     * we unpermute a number */
    if (offset > 0)
    {
      reverse_masks.push_back(mask << offset);
    }
    else
    {
      reverse_masks.push_back(mask >> -offset);
    }
  }

  /* Alright, we have to determine the low and high masks for this
   * particular table. If we are searching for up to /d/ differing bits,
   * then we should  include all but the last /d/ blocks in our mask.
   *
   * After this, width should hold the number of bits that are in all but
   * the last d blocks */
  std::vector<size_t>::iterator width_it(widths.begin());
  for (width = 0; different_bits < widths.size();
       ++different_bits, ++width_it)
  {
    width += *width_it;
  }

  /* Set the first /width/ bits in the low mask to 1, and then shift it up
   * until it's a full 64-bit number. */
  for (i = 0; i < width; ++i)
  {
    search_mask_ = (search_mask_ << 1) | 1;
  }
  for (i = width; i < 64; ++i)
  {
    search_mask_ = search_mask_ << 1;
  }
}

Simhash::hash_t Simhash::Permutation::apply(hash_t hash) const
{
  std::vector<hash_t>::const_iterator masks_it(forward_masks.begin());
  std::vector<int>::const_iterator offset_it(offsets.begin());

  hash_t result(0);
  for (; masks_it != forward_masks.end(); ++masks_it, ++offset_it)
  {
    if (*offset_it > 0)
    {
      result = result | ((hash & *masks_it) << *offset_it);
    }
    else
    {
      result = result | ((hash & *masks_it) >> -(*offset_it));
    }
  }
  return result;
}

Simhash::hash_t Simhash::Permutation::reverse(hash_t hash) const
{
  std::vector<hash_t>::const_iterator masks_it(reverse_masks.begin());
  std::vector<int>::const_iterator offset_it(offsets.begin());

  hash_t result(0);
  for (; masks_it != reverse_masks.end(); ++masks_it, ++offset_it)
  {
    if (*offset_it > 0)
    {
      result = result | ((hash & *masks_it) >> *offset_it);
    }
    else
    {
      result = result | ((hash & *masks_it) << -(*offset_it));
    }
  }
  return result;
}

Simhash::hash_t Simhash::Permutation::search_mask() const
{
  return search_mask_;
}