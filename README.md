# Introduction

This is the implementation of the paper, "WIPE, a write-optimized learned index for persistent memory".

## Compile & Run

### Dependencies

- intel-mkl
- libpmem
- libpmemobj

### Configuration

Please set the PM pool path and dataset path before testing.

Suppose the NVDIMM is mounted at `/mnt/pmem1`, we use `numactl` to bind the process.

### Build

```bash
git clone https://github.com/olemon111/WIPE
cd WIPE
./build.sh
./test/run_example.sh
```
See `tests/run_example.sh` for more test details.

## Datasets

- Longlat
- YCSB
- Longitudes
- Lognormal

## Other Indexes

- [FAST&FAIR](https://github.com/DICL/FAST_FAIR)
- [APEX](https://github.com/baotonglu/apex)
- [ALEX](https://github.com/microsoft/ALEX)
- [PGM](https://github.com/gvinciguerra/PGM-index)
- [XIndex](https://ipads.se.sjtu.edu.cn:1312/opensource/xindex)
- [LIPP](https://github.com/Jiacheng-WU/lipp)