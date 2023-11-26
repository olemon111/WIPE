

#ifndef _LINUX_BITOPS_H
#define _LINUX_BITOPS_H



static inline void __set_bit(unsigned long nr,  const void *addr)
{
	unsigned int *a = (unsigned int *) addr;
	int mask;

	a += nr >> 5;
	mask = 1 << (nr & 0x1f);
	*a |= mask;
}

static inline void __clear_bit(unsigned long nr,  const void *addr)
{
	unsigned int *a = (unsigned int *) addr;
	int mask;

	a += nr >> 5;
	mask = 1 << (nr & 0x1f);
	*a &= ~mask;
}

/*
 * test bit
 */
static inline int test_bit(unsigned long nr, const  void *addr)
{
	return 1UL & (((const  unsigned int *) addr)[nr >> 5] >> (nr & 31));
}

static inline int find_bit(unsigned int num) {
    int offset = 0;
    if(num == 0) return 32;
    while(!(num & 1)) {
        num >>= 1;
        offset ++;
    }
    return offset;
}

static inline int find_first_bit(const void *vaddr, unsigned size)
{
	const unsigned int *p = (unsigned int *) vaddr;
	int res = 32;
	unsigned int words;
	unsigned long num;

	if (!size)
		return 0;

	words = (size + 31) >> 5;
	while (!(num = *p++)) {
		if (!--words)
			goto out;
	}
	res = find_bit(num & -num);
out:
	res += ((long)p - (long)vaddr - 4) * 8;
	return res < size ? res : size;
}

static inline int find_first_zero_bit(const void *vaddr, unsigned size)
{
	const unsigned int *p = (unsigned int *) vaddr;
	int res = 32;
	unsigned int words;
	unsigned long num;

	if (!size)
		return 0;

	words = (size + 31) >> 5;
	while (!(~(num = *p++))) {
		if (!--words)
			goto out;
	}
	res = find_bit(~num);
out:
	res += ((long)p - (long)vaddr - 4) * 8;
	return res < size ? res : size;
}

static inline int find_next_bit(const  void *addr, int size,
				int offset)
{
	const unsigned int *p = (unsigned int *) addr;
	int bit = offset & 31U, res;
    p += (offset >> 5);

	if (offset >= size)
		return size;

	if (bit) {
		unsigned int num = *p++ & (~0U << bit);
		offset -= bit;
        unsigned int tmp = num & (-num);
        if(tmp == 0) res = 32;
        else {
            res = find_bit(tmp);
        }
        
		if (res < 32) {
			offset += res;
			return offset < size ? offset : size;
		}
		offset += 32;

		if (offset >= size)
			return size;
	}
	/* No one yet, search remaining full bytes for a one */
	return offset + find_first_bit(p, size - offset);
}


#define set_bit __set_bit
#define clear_bit __clear_bit
#endif /* _LINUX_BITOPS_H */
