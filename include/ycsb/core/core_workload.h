//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <vector>
#include <string>
#include "db.h"
#include "properties.h"
#include "generator.h"
#include "discrete_generator.h"
#include "counter_generator.h"
#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"

#include <string>
#include "utils.h"

namespace ycsbc {

enum Operation {
  INSERT,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE,
  NR_OPERATIONS,
};

const char *OperationName[] = {
  "INSERT",
  "READ",
  "UPDATE",
  "SCAN",
  "READMODIFYWRITE"
};

class CoreWorkload {
 public:
  /// 
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;
  
  /// 
  /// The name of the property for the number of fields in a record.
  ///
  static const std::string FIELD_COUNT_PROPERTY;
  static const std::string FIELD_COUNT_DEFAULT;
  
  /// 
  /// The name of the property for the field length distribution.
  /// Options are "uniform", "zipfian" (favoring short records), and "constant".
  ///
  static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;
  
  /// 
  /// The name of the property for the length of a field in bytes.
  ///
  static const std::string FIELD_LENGTH_PROPERTY;
  static const std::string FIELD_LENGTH_DEFAULT;
  
  /// 
  /// The name of the property for deciding whether to read one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string READ_ALL_FIELDS_PROPERTY;
  static const std::string READ_ALL_FIELDS_DEFAULT;

  /// 
  /// The name of the property for deciding whether to write one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string WRITE_ALL_FIELDS_PROPERTY;
  static const std::string WRITE_ALL_FIELDS_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;
  
  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;
  
  /// 
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;
  
  /// 
  /// The name of the property for the scan length distribution.
  /// Options are "uniform" and "zipfian" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

  /// 
  /// The name of the property for the order to insert records.
  /// Options are "ordered" or "hashed".
  ///
  static const std::string INSERT_ORDER_PROPERTY;
  static const std::string INSERT_ORDER_DEFAULT;

  static const std::string INSERT_START_PROPERTY;
  static const std::string INSERT_START_DEFAULT;
  
  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties &p);
  virtual void Reload(const utils::Properties &p);
  
  virtual void BuildValues(std::vector<ycsbc::DB::KVPair> &values);
  virtual void BuildUpdate(std::vector<ycsbc::DB::KVPair> &update);
  
  virtual std::string NextTable() { return table_name_; }
  virtual std::string NextSequenceKey(); /// Used for loading data
  virtual std::string NextTransactionKey(); /// Used for transactions
  virtual uint64_t NextSequenceIntKey();  /// Used for loading data int64
  virtual uint64_t NextTransactionIntKey(); /// Used for transactions int 64

  virtual Operation NextOperation() { return op_chooser_.Next(); }
  virtual std::string NextFieldName();
  virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }
  
  bool read_all_fields() const { return read_all_fields_; }
  bool write_all_fields() const { return write_all_fields_; }

  CoreWorkload() :
      field_count_(0), read_all_fields_(false), write_all_fields_(false),
      field_len_generator_(NULL), key_generator_(NULL), key_chooser_(NULL),
      field_chooser_(NULL), scan_len_chooser_(NULL), insert_key_sequence_(3),
      ordered_inserts_(true), record_count_(0) {
  }
  
  virtual ~CoreWorkload() {
    if (field_len_generator_) delete field_len_generator_;
    if (key_generator_) delete key_generator_;
    if (key_chooser_) delete key_chooser_;
    if (field_chooser_) delete field_chooser_;
    if (scan_len_chooser_) delete scan_len_chooser_;
  }
  
 protected:
  static Generator<uint64_t> *GetFieldLenGenerator(const utils::Properties &p);
  std::string BuildKeyName(uint64_t key_num);

  std::string table_name_;
  int field_count_;
  bool read_all_fields_;
  bool write_all_fields_;
  Generator<uint64_t> *field_len_generator_;
  Generator<uint64_t> *key_generator_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t> *key_chooser_;
  Generator<uint64_t> *field_chooser_;
  Generator<uint64_t> *scan_len_chooser_;
  CounterGenerator insert_key_sequence_;
  bool ordered_inserts_;
  size_t record_count_;
};

inline std::string CoreWorkload::NextSequenceKey() {
  uint64_t key_num = key_generator_->Next();
  return BuildKeyName(key_num);
}

inline std::string CoreWorkload::NextTransactionKey() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  return BuildKeyName(key_num);
}

inline uint64_t CoreWorkload::NextSequenceIntKey() {
  uint64_t key_num = key_generator_->Next();
  return utils::Hash(key_num);
}

inline uint64_t CoreWorkload::NextTransactionIntKey() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  return utils::Hash(key_num);
}

inline std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  return std::string("user").append(std::to_string(key_num));
}

inline std::string CoreWorkload::NextFieldName() {
  return std::string("field").append(std::to_string(field_chooser_->Next()));
}

const std::string CoreWorkload::TABLENAME_PROPERTY = "table";
const std::string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const std::string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const std::string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const std::string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY =
    "field_len_dist";
const std::string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const std::string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const std::string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const std::string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const std::string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const std::string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const std::string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const std::string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const std::string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const std::string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const std::string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const std::string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const std::string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const std::string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const std::string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const std::string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY =
    "readmodifywriteproportion";
const std::string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const std::string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY =
    "requestdistribution";
const std::string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const std::string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const std::string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const std::string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY =
    "scanlengthdistribution";
const std::string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const std::string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const std::string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const std::string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const std::string CoreWorkload::INSERT_START_DEFAULT = "0";

const std::string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const std::string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

void CoreWorkload::Init(const utils::Properties &p) {
  table_name_ = p.GetProperty(TABLENAME_PROPERTY,TABLENAME_DEFAULT);
  
  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY,
                                         FIELD_COUNT_DEFAULT));
  field_len_generator_ = GetFieldLenGenerator(p);
  
  double read_proportion = std::stod(p.GetProperty(READ_PROPORTION_PROPERTY,
                                                   READ_PROPORTION_DEFAULT));
  double update_proportion = std::stod(p.GetProperty(UPDATE_PROPORTION_PROPERTY,
                                                     UPDATE_PROPORTION_DEFAULT));
  double insert_proportion = std::stod(p.GetProperty(INSERT_PROPORTION_PROPERTY,
                                                     INSERT_PROPORTION_DEFAULT));
  double scan_proportion = std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY,
                                                   SCAN_PROPORTION_DEFAULT));
  double readmodifywrite_proportion = std::stod(p.GetProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));
  
  record_count_ = std::stoi(p.GetProperty(RECORD_COUNT_PROPERTY));
  std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                           REQUEST_DISTRIBUTION_DEFAULT);
  int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY,
                                             MAX_SCAN_LENGTH_DEFAULT));
  std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                            SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  int insert_start = std::stoi(p.GetProperty(INSERT_START_PROPERTY,
                                             INSERT_START_DEFAULT));
  
  read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                    READ_ALL_FIELDS_DEFAULT));
  write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                     WRITE_ALL_FIELDS_DEFAULT));
  
  if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
    ordered_inserts_ = false;
  } else {
    ordered_inserts_ = true;
  }
  
  key_generator_ = new CounterGenerator(insert_start);
  
  if (read_proportion > 0) {
    op_chooser_.AddValue(READ, read_proportion);
  }
  if (update_proportion > 0) {
    op_chooser_.AddValue(UPDATE, update_proportion);
  }
  if (insert_proportion > 0) {
    op_chooser_.AddValue(INSERT, insert_proportion);
  }
  if (scan_proportion > 0) {
    op_chooser_.AddValue(SCAN, scan_proportion);
  }
  if (readmodifywrite_proportion > 0) {
    op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
  }
  
  insert_key_sequence_.Set(record_count_);
  
  if (request_dist == "uniform") {
    key_chooser_ = new UniformGenerator(0, record_count_ - 1);
    
  } else if (request_dist == "zipfian") {
    // If the number of keys changes, we don't want to change popular keys.
    // So we construct the scrambled zipfian generator with a keyspace
    // that is larger than what exists at the beginning of the test.
    // If the generator picks a key that is not inserted yet, we just ignore it
    // and pick another key.
    int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    int new_keys = (int)(op_count * insert_proportion * 2); // a fudge factor
    key_chooser_ = new ScrambledZipfianGenerator(record_count_ + new_keys);
    
  } else if (request_dist == "latest") {
    key_chooser_ = new SkewedLatestGenerator(insert_key_sequence_);
    
  } else {
    throw utils::Exception("Unknown request distribution: " + request_dist);
  }
  
  field_chooser_ = new UniformGenerator(0, field_count_ - 1);
  
  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = new UniformGenerator(1, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = new ZipfianGenerator(1, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " +
        scan_len_dist);
  }
}

void CoreWorkload::Reload(const utils::Properties &p) {
  table_name_ = p.GetProperty(TABLENAME_PROPERTY,TABLENAME_DEFAULT);
  
  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY,
                                         FIELD_COUNT_DEFAULT));
  field_len_generator_ = GetFieldLenGenerator(p);
  
  double read_proportion = std::stod(p.GetProperty(READ_PROPORTION_PROPERTY,
                                                   READ_PROPORTION_DEFAULT));
  double update_proportion = std::stod(p.GetProperty(UPDATE_PROPORTION_PROPERTY,
                                                     UPDATE_PROPORTION_DEFAULT));
  double insert_proportion = std::stod(p.GetProperty(INSERT_PROPORTION_PROPERTY,
                                                     INSERT_PROPORTION_DEFAULT));
  double scan_proportion = std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY,
                                                   SCAN_PROPORTION_DEFAULT));
  double readmodifywrite_proportion = std::stod(p.GetProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));
  
  // record_count_ = std::stoi(p.GetProperty(RECORD_COUNT_PROPERTY)); // ? replace with: 
  record_count_ = key_generator_->Last();
  insert_key_sequence_.Set(record_count_);;
  std::cout << "Record: " << record_count_ << std::endl;
  std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                           REQUEST_DISTRIBUTION_DEFAULT);
  int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY,
                                             MAX_SCAN_LENGTH_DEFAULT));
  std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                            SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  // int insert_start = std::stoi(p.GetProperty(INSERT_START_PROPERTY,
  //                                            INSERT_START_DEFAULT));
  
  read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                    READ_ALL_FIELDS_DEFAULT));
  write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                     WRITE_ALL_FIELDS_DEFAULT));
  
  if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
    ordered_inserts_ = false;
  } else {
    ordered_inserts_ = true;
  }
  op_chooser_.Clear();
  if (read_proportion > 0) {
    op_chooser_.AddValue(READ, read_proportion);
  }
  if (update_proportion > 0) {
    op_chooser_.AddValue(UPDATE, update_proportion);
  }
  if (insert_proportion > 0) {
    op_chooser_.AddValue(INSERT, insert_proportion);
  }
  if (scan_proportion > 0) {
    op_chooser_.AddValue(SCAN, scan_proportion);
  }
  if (readmodifywrite_proportion > 0) {
    op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
  }
  
  if (request_dist == "uniform") {
    key_chooser_ = new UniformGenerator(0, record_count_ - 1);

  } else if (request_dist == "zipfian") {
    // If the number of keys changes, we don't want to change popular keys.
    // So we construct the scrambled zipfian generator with a keyspace
    // that is larger than what exists at the beginning of the test.
    // If the generator picks a key that is not inserted yet, we just ignore it
    // and pick another key.
    int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    int new_keys = (int)(op_count * insert_proportion * 2); // a fudge factor
    key_chooser_ = new ScrambledZipfianGenerator(record_count_ + new_keys);
    
  } else if (request_dist == "latest") {
    key_chooser_ = new SkewedLatestGenerator(insert_key_sequence_);
    
  } else {
    throw utils::Exception("Unknown request distribution: " + request_dist);
  }
  
  field_chooser_ = new UniformGenerator(0, field_count_ - 1);
  
  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = new UniformGenerator(1, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = new ZipfianGenerator(1, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " +
        scan_len_dist);
  }
}

ycsbc::Generator<uint64_t> *CoreWorkload::GetFieldLenGenerator(
    const utils::Properties &p) {
  std::string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                        FIELD_LENGTH_DISTRIBUTION_DEFAULT);
  int field_len = std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY,
                                          FIELD_LENGTH_DEFAULT));
  if(field_len_dist == "constant") {
    return new ConstGenerator(field_len);
  } else if(field_len_dist == "uniform") {
    return new UniformGenerator(1, field_len);
  } else if(field_len_dist == "zipfian") {
    return new ZipfianGenerator(1, field_len);
  } else {
    throw utils::Exception("Unknown field length distribution: " +
        field_len_dist);
  }
}

void CoreWorkload::BuildValues(std::vector<ycsbc::DB::KVPair> &values) {
  for (int i = 0; i < field_count_; ++i) {
    ycsbc::DB::KVPair pair;
    pair.first.append("field").append(std::to_string(i));
    pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
    values.push_back(pair);
  }
}

void CoreWorkload::BuildUpdate(std::vector<ycsbc::DB::KVPair> &update) {
  ycsbc::DB::KVPair pair;
  pair.first.append(NextFieldName());
  pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
  update.push_back(pair);
}

} // ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
