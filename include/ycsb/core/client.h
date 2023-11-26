//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {
    for(int i = 0; i < NR_OPERATIONS; i++) {
      OP_counts[i] = 0;
    }
  }
  
  virtual bool DoInsert();
  virtual bool DoTransaction();
  
  virtual ~Client() {
    for(int i = 0; i < NR_OPERATIONS; i++) {
      std::cerr << OperationName[i] << ": " << OP_counts[i] << ". ";
    }
    std::cerr << std::endl;
  }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  size_t OP_counts[NR_OPERATIONS];
  DB &db_;
  CoreWorkload &workload_;
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  int op;
  switch ((op = workload_.NextOperation())) {
    case READ:
      status = TransactionRead();
      break;
    case UPDATE:
      status = TransactionUpdate();
      break;
    case INSERT:
      status = TransactionInsert();
      break;
    case SCAN:
      status = TransactionScan();
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  OP_counts[op] ++;
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values);
} 

template <class db_t>
class KvClient {

public:
  KvClient(db_t *db, CoreWorkload &wl) : db_(db), workload_(wl) {
    for(int i = 0; i < NR_OPERATIONS; i++) {
      OP_counts[i] = 0;
    }
  }
  
  virtual bool DoInsert();
  virtual bool DoTransaction();
  
  virtual ~KvClient() {
    for(int i = 0; i < NR_OPERATIONS; i++) {
      std::cerr << OperationName[i] << ": " << OP_counts[i] << ". ";
    }
    std::cerr << std::endl;
  }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  db_t *db_;
  CoreWorkload &workload_;
  size_t OP_counts[NR_OPERATIONS];
};

template <class db_t>
inline bool KvClient<db_t>::DoInsert() {
  OP_counts[INSERT] ++;
  uint64_t key = workload_.NextSequenceIntKey();
  return db_->Put(key, key);
}

template <class db_t>
inline bool KvClient<db_t>::DoTransaction() {
  int status = -1;
  int op;
  switch ((op = workload_.NextOperation())) {
    case READ:
      status = TransactionRead();
      break;
    case UPDATE:
      status = TransactionUpdate();
      break;
    case INSERT:
      status = TransactionInsert();
      break;
    case SCAN:
      status = TransactionScan();
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  OP_counts[op] ++;
  assert(status >= 0);
  return (status == DB::kOK);
}

template <class db_t>
inline int KvClient<db_t>::TransactionRead() {
  const uint64_t &key = workload_.NextTransactionIntKey();
  uint64_t value;
  return db_->Get(key, value);
}

template <class db_t>
inline int KvClient<db_t>::TransactionReadModifyWrite() {
  const uint64_t &key = workload_.NextTransactionIntKey();
  uint64_t value;
  db_->Get(key, value);

  return db_->Update(key, value);
}

template <class db_t>
inline int KvClient<db_t>::TransactionScan() {
  const uint64_t &key = workload_.NextTransactionIntKey();
  int len = workload_.NextScanLength();
  std::vector<std::pair<uint64_t, uint64_t>> results;
  return db_->Scan(key, len, results);
}

template <class db_t>
inline int KvClient<db_t>::TransactionUpdate() {
  const uint64_t &key = workload_.NextTransactionIntKey();
  uint64_t value = key;
  return db_->Update(key, value);
}

template <class db_t>
inline int KvClient<db_t>::TransactionInsert() {
  uint64_t key = workload_.NextSequenceIntKey();
  return db_->Put(key, key);
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
