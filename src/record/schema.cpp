#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  memcpy(buf,&SCHEMA_MAGIC_NUM,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  uint32_t size = columns_.size();
  memcpy(buf,&size,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  for(auto ptr:columns_){
    uint32_t length = ptr->SerializeTo(buf);
    buf += length;
    offset += length;
  }
  memcpy(buf,&is_manage_,sizeof(bool));
  buf += sizeof(bool);
  offset += sizeof(bool);
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t length = 2*sizeof(uint32_t)+sizeof(bool);
  for(auto ptr:columns_){
    length += ptr->GetSerializedSize();
  }
  return length;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  if(schema!=nullptr){
    LOG(WARNING)<<"Pointer to schema is not null in schema deserialize."<<std::endl;
  }
  uint32_t offset = 0;
  uint32_t MAGIC_NUM;
  memcpy(&MAGIC_NUM,buf,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);

  uint32_t size;
  memcpy(&size,buf,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);

  std::vector<Column*> column;
  for(uint32_t i=0;i<size;i++){
    Column* ptr = new Column();
    ptr = nullptr;
    uint32_t length = Column::DeserializeFrom(buf,ptr);
    column.push_back(ptr);
    buf += length;
    offset += length;
  }
  schema = new Schema(column,false);

  memcpy(&schema->is_manage_,buf,sizeof(bool));
  buf += sizeof(bool);
  offset += sizeof(bool);
  return offset;
}