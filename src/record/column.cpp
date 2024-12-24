#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  memcpy(buf,&COLUMN_MAGIC_NUM,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);

  uint32_t length = name_.length();
  memcpy(buf,&length,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);

  const char* p = name_.c_str();
  memcpy(buf,p,sizeof(char)*length);
  buf += sizeof(char)*length;
  offset += sizeof(char)*length;

  memcpy(buf,&type_,sizeof(TypeId));
  buf += sizeof(TypeId);
  offset += sizeof(TypeId);

  memcpy(buf,&len_, sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);

  memcpy(buf,&table_ind_, sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);

  memcpy(buf,&nullable_,sizeof(bool));
  buf += sizeof(bool);
  offset += sizeof(bool);

  memcpy(buf,&unique_, sizeof(bool));
  buf += sizeof(bool);
  offset += sizeof(bool);
  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return 4*sizeof(uint32_t)+sizeof(char)*name_.length()+sizeof(TypeId)+2*sizeof(bool);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if(column!= nullptr){
    LOG(WARNING)<<"Pointer to column is not null in column deserialize."<<std::endl;
  }
  uint32_t offset = 0;
  uint32_t MAGIC_NUM;
  memcpy(&MAGIC_NUM,buf,sizeof(uint32_t));
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  uint32_t length;
  memcpy(&length,buf,sizeof(uint32_t));
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);

  char* p = new char[length];
  memcpy(p,buf,sizeof(char)*length);
  std::string name = p;
  buf += sizeof(char)*length;
  offset += sizeof(char)*length;
  delete[] p;

  TypeId type;
  memcpy(&type,buf,sizeof(TypeId));
  offset += sizeof(TypeId);
  buf += sizeof(TypeId);

  uint32_t len;
  memcpy(&len,buf,sizeof(uint32_t));
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  uint32_t table_ind;
  memcpy(&table_ind,buf,sizeof(uint32_t));
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);

  bool nullable;
  memcpy(&nullable,buf,sizeof(bool));
  offset += sizeof(bool);
  buf += sizeof(bool);

  bool unique;
  memcpy(&unique,buf,sizeof(bool));
  offset += sizeof(bool);
  buf += sizeof(bool);
  if(type==kTypeChar){
    column = new Column(name,type,len,table_ind,nullable,unique);
  }else{
    column = new Column(name,type,table_ind,nullable,unique);
  }
  return offset;
}
