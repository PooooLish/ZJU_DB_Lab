#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages):page_num(num_pages),occupied_num(0){}

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

  size_t Occupied_Size(){
      return occupied_num;
  };

private:
  class page{
    public:
      bool is_pinned;
      frame_id_t frame_id;
  };
  size_t page_num;
  size_t occupied_num;
  vector<class page> buffer_pool;
  // add your own private member variables here
};

#endif  // MINISQL_LRU_REPLACER_H
