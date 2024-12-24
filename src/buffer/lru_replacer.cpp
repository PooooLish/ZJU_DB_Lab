#include "buffer/lru_replacer.h"

//LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(!buffer_pool.empty()){
    for(auto i=buffer_pool.begin();i!=buffer_pool.end();i++){
      if(!i->is_pinned){
        *frame_id = i->frame_id;
        buffer_pool.erase(i);
        occupied_num -= 1;
        return true;
      }
    }
    return false;
  }else{
    *frame_id = -1;
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  page new_page;
  for(auto i=buffer_pool.begin();i<buffer_pool.end();i++){
    if(i->frame_id==frame_id && !i->is_pinned){
      new_page = {true,i->frame_id};
      buffer_pool.erase(i);
      buffer_pool.push_back(new_page);
      break;
    }
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  page new_page;
  bool need_to_add= true;
  for(auto i=buffer_pool.begin();i<buffer_pool.end();i++){
    if(i->frame_id==frame_id){
      need_to_add = false;
      new_page = {false,i->frame_id};
      if(i->is_pinned){
        buffer_pool.erase(i);
        buffer_pool.push_back(new_page);
      }
      break;
    }
  }
  if(need_to_add && buffer_pool.size()<page_num){
    new_page = {false,frame_id};
    buffer_pool.push_back(new_page);
    occupied_num += 1;
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  size_t count = 0;
  for(auto frameptr:buffer_pool){
    if(!frameptr.is_pinned) count += 1;
  }
  return count;
}