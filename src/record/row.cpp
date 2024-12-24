#include "record/row.h"
#include <iostream>

/**
 * TODO: Student Implement
 */
 /**
  Schema is used to debug.
//  */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t offset = 0;
  uint32_t size = fields_.size();
  memcpy(buf,&size,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  for(int i=0;i<size;i++){
    uint32_t length = fields_[i]->SerializeTo(buf);
    buf += length;
    offset += length;
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t offset = 0;
  uint32_t size;
  memcpy(&size,buf,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  for(int i=0;i<size;i++){
    Field* temp = new Field;
    uint32_t length = fields_[i]->DeserializeFrom(buf,schema->GetColumns()[i]->GetType(),&temp,false);
    fields_.push_back(temp);
    buf += length;
    offset += length;
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t length = sizeof(uint32_t);
  for(auto ptr:this->fields_){
    length += ptr->GetSerializedSize();
  }
  return length;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
