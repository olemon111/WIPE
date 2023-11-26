#include "getopt.h"
#include "db_interface.h"
#include "util.h"

using letree::Random;
using ycsbc::KvDB;
using namespace dbInter;
using namespace std;

void show_help(char *prog)
{
  cout << "Usage: " << prog << " [options]" << endl
       << endl
       << "  Option:" << endl
       << "    --load-size              LOAD_SIZE" << endl
       << "    --put-size               PUT_SIZE" << endl
       << "    --get-size               GET_SIZE" << endl
       << "    --help[-h]               show help" << endl;
}

vector<uint64_t> generate_uniform_random(size_t op_num)
{
  vector<uint64_t> data;
  data.resize(op_num);
  const uint64_t ns = util::timing([&]
                                   {
                                     Random rnd(0, UINT64_MAX);
                                     for (size_t i = 0; i < op_num; ++i)
                                     {
                                       data[i] = rnd.Next();
                                     } });

  const uint64_t ms = ns / 1e6;
  cout << "generate " << data.size() << " values in "
       << ms << " ms (" << static_cast<double>(data.size()) / 1000 / ms
       << " M values/s)" << endl;
  return data;
}

int main(int argc, char *argv[])
{
  int thread_num = 1;
  size_t LOAD_SIZE = 10000000;
  size_t PUT_SIZE = 10000000;
  size_t GET_SIZE = 10000000;

  static struct option opts[] = {
      /* NAME               HAS_ARG            FLAG  SHORTNAME*/
      {"load-size", required_argument, NULL, 0},
      {"put-size", required_argument, NULL, 0},
      {"get-size", required_argument, NULL, 0},
      {"help", no_argument, NULL, 'h'},
      {NULL, 0, NULL, 0}};

  // parse arguments
  int c;
  int opt_idx;
  string load_file = "";
  while ((c = getopt_long(argc, argv, "n:dh", opts, &opt_idx)) != -1)
  {
    switch (c)
    {
    case 0:
      switch (opt_idx)
      {
      case 0:
        LOAD_SIZE = atoi(optarg);
        break;
      case 1:
        PUT_SIZE = atoi(optarg);
        break;
      case 2:
        GET_SIZE = atoi(optarg);
        break;
      case 'h':
        show_help(argv[0]);
        return 0;
      default:
        cout << (char)c << endl;
        abort();
      }
    }
  }
  cout << "LOAD_SIZE:             " << LOAD_SIZE << endl;
  cout << "PUT_SIZE:              " << PUT_SIZE << endl;
  cout << "GET_SIZE:              " << GET_SIZE << endl;

  vector<uint64_t> data_base = generate_uniform_random(LOAD_SIZE + PUT_SIZE * 10);
  NVM::env_init();
  KvDB *db = new LetDB();
  db->Init();
  uint64_t load_pos = 0;
  // load
  cout << "start loading ...." << endl;
  util::FastRandom ranny(18);
  for (load_pos; load_pos < LOAD_SIZE; load_pos++)
  {
    db->Put(data_base[load_pos], (uint64_t)data_base[load_pos]);
  }
  load_pos = LOAD_SIZE;
  // test put
  for (int i = 0; i < PUT_SIZE; i++)
  {
    db->Put(data_base[load_pos], (uint64_t)data_base[load_pos]);
    load_pos++;
  }
  cout << "test put " << PUT_SIZE << " kvs." << endl;
  // test get
  vector<uint64_t> rand_pos;
  for (uint64_t i = 0; i < GET_SIZE; i++)
  {
    rand_pos.push_back(ranny.RandUint32(0, load_pos - 1));
  }

  int wrong_get = 0;
  uint64_t value = 0;
  for (uint64_t i = 0; i < GET_SIZE; i++)
  {
    db->Get(data_base[rand_pos[i]], value);
    if (value != data_base[rand_pos[i]])
    {
      wrong_get++;
    }
  }
  cout << "test get " << GET_SIZE << " kvs, with " << wrong_get << " wrong value." << endl;

  delete db;
  NVM::env_exit();

  return 0;
}
