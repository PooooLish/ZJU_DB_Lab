#include "page/bitmap_page.h"

#include "glog/logging.h"

#include<cmath>
/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset){
  if(next_free_page_<GetMaxSupportedSize()){
    bytes[next_free_page_/8] |= 1<<(next_free_page_%8);
    page_offset = next_free_page_;
    page_allocated_ += 1;
    for(;next_free_page_<GetMaxSupportedSize();next_free_page_++){
      if(!(bytes[next_free_page_/8]&(1<<(next_free_page_%8)))) break;
    }
    return true;
  }else return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  int flag = 1 << (page_offset%8);
  if(!IsPageFree(page_offset)){
    bytes[page_offset/8]&=~flag;
    page_allocated_ -= 1;
    if(next_free_page_>page_offset) next_free_page_=page_offset;
    return true;
  }else return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  int flag = 1 << (page_offset%8);
  if(bytes[page_offset/8]&flag) return false;
  else return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;