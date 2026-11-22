// Shopping Sessions: native pipeline.
//
// Layout of the run (T worker threads, all phases parallel):
//   phase 1  mmap every CSV, split into ~1 MB line-aligned chunks and parse them
//            with a SIMD structural scanner. Events are radix-scattered by
//            event_id hash, valid clicks are kept per thread. Thread 0 builds
//            the linked-ID union-find, thread 1 the product table (clicks are
//            parsed first so event chunks never wait on it).
//   phase 2  per event_id partition: dedup (smallest ingest_ts wins), keep the
//            valid winners, map user -> person, scatter by person hash.
//            Clicks are mapped user -> person and scattered the same way.
//   phase 3  per person partition: group, sort by ts, sessionize, attribute
//            with a sliding 7-day click window, aggregate. Each thread writes
//            its own user_daily Parquet part; campaign totals are merged last.
//
// IDs are keyed by a 128-bit value: strings up to 15 bytes are packed exactly
// (bytes + length), longer ones are hashed into a disjoint key space.

#include <algorithm>
#include <atomic>
#include <mutex>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

typedef uint8_t u8;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int32_t i32;
typedef int64_t i64;

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)

static const i64 DAY_MS = 86400000LL;
static const i64 GAP_MS = 1800000LL;
static const i64 WEEK_MS = 7 * DAY_MS;

// ---------------------------------------------------------------------------
// small helpers

static inline u64 load64(const char* p) {
  u64 v;
  memcpy(&v, p, 8);
  return v;
}

struct Key {
  u64 lo, hi;
};

static inline u64 mum(u64 a, u64 b) {
  __uint128_t m = (__uint128_t)a * b;
  return (u64)m ^ (u64)(m >> 64);
}

static inline u64 hkey(const Key& k) {
  return mum(k.lo ^ 0x9E3779B97F4A7C15ULL, k.hi ^ 0xD1B54A32D192ED03ULL);
}

static Key make_key_long(const char* p, u32 n) {
  u64 h1 = 0x243F6A8885A308D3ULL ^ n, h2 = 0x13198A2E03707344ULL + n;
  u32 i = 0;
  for (; i + 8 <= n; i += 8) {
    u64 w = load64(p + i);
    h1 = mum(h1 ^ w, 0xA0761D6478BD642FULL);
    h2 = mum(h2 + w, 0xE7037ED1A0B428DBULL) + w;
  }
  if (i < n) {
    u64 w = load64(p + i) & (~0ULL >> (64 - 8 * (n - i)));
    h1 = mum(h1 ^ w, 0xA0761D6478BD642FULL);
    h2 = mum(h2 + w, 0xE7037ED1A0B428DBULL) + w;
  }
  h1 = mum(h1 ^ (h2 >> 7), 0x8EBC6AF09C88C6E3ULL) ^ h2;
  h2 = mum(h2 ^ 0x589965CC75374CC3ULL, h1 | 1) + h1;
  return {h1, (h2 & 0x00FFFFFFFFFFFFFFULL) | (0xF0ULL << 56)};
}

// Requires at least 16 readable bytes from p (all buffers are padded).
static inline Key make_key(const char* p, u32 n) {
  if (LIKELY(n <= 15)) {
    u64 a = load64(p), b = load64(p + 8);
    if (n < 8) {
      a = n ? (a & (~0ULL >> (64 - 8 * n))) : 0;
      b = 0;
    } else {
      b = (n == 8) ? 0 : (b & (~0ULL >> (64 - 8 * (n - 8))));
    }
    return {a, b | ((u64)n << 56)};
  }
  return make_key_long(p, n);
}

static inline bool key_eq(const Key& a, const Key& b) { return a.lo == b.lo && a.hi == b.hi; }

static inline bool str_less(const char* a, u32 la, const char* b, u32 lb) {
  u32 m = la < lb ? la : lb;
  int c = memcmp(a, b, m);
  if (c) return c < 0;
  return la < lb;
}

static inline bool isd(char c) { return (unsigned)(c - '0') < 10u; }
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }

static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
  y -= m <= 2;
  const i64 era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (i64)doe - 719468;
}

static inline bool is_leap(int y) { return (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0); }

static inline int days_in_month(int y, int m) {
  static const int dm[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  return (m == 2 && is_leap(y)) ? 29 : dm[m];
}

// Parse a `ts` value. Returns false for any malformed form. F3 values that do
// not fit a signed 64-bit integer are reported as malformed too (they are
// outside every window anyway).
// Per-thread cache of the last valid date prefix "YYYY-MM-DD" -> epoch day.
struct DateCache {
  u64 a = 0;
  u32 b = 0;
  i64 days = 0;
};
static thread_local DateCache t_date;

static inline bool parse_ts(const char* p, u32 n, i64& out) {
  if ((n == 20 || n == 25) && p[4] == '-') {
    i64 days;
    u64 a = load64(p);
    u32 b;
    memcpy(&b, p + 8, 2);
    b &= 0xFFFF;
    DateCache& dc = t_date;
    if (LIKELY(a == dc.a && b == dc.b)) {
      days = dc.days;
    } else {
      if (p[7] != '-') return false;
      if (!((int)isd(p[0]) & isd(p[1]) & isd(p[2]) & isd(p[3]) & isd(p[5]) & isd(p[6]) & isd(p[8]) & isd(p[9])))
        return false;
      int y = d2(p) * 100 + d2(p + 2);
      int mo = d2(p + 5), d = d2(p + 8);
      if (mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo)) return false;
      days = days_from_civil(y, mo, d);
      dc.a = a;
      dc.b = b;
      dc.days = days;
    }
    if (p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
    if (!((int)isd(p[11]) & isd(p[12]) & isd(p[14]) & isd(p[15]) & isd(p[17]) & isd(p[18]))) return false;
    int h = d2(p + 11), mi = d2(p + 14), sc = d2(p + 17);
    if (h > 23 || mi > 59 || sc > 59) return false;
    i64 off = 0;
    if (n == 20) {
      if (p[19] != 'Z') return false;
    } else {
      char sg = p[19];
      if (sg != '+' && sg != '-') return false;
      if (!((int)isd(p[20]) & isd(p[21]) & isd(p[23]) & isd(p[24])) || p[22] != ':') return false;
      int oh = d2(p + 20), om = d2(p + 23);
      if (oh > 23 || om > 59) return false;
      off = (i64)(oh * 60 + om) * 60;
      if (sg == '-') off = -off;
    }
    out = ((days * 86400 + h * 3600 + mi * 60 + sc) - off) * 1000;
    return true;
  }
  if (n == 0) return false;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  u32 r = n - i;
  if (r > 19) {
    for (u32 j = i; j < n; j++)
      if (!isd(p[j])) return false;
    return false;  // digits but beyond int64: outside any window
  }
  u64 v = 0;
  for (u32 j = i; j < n; j++) {
    unsigned c = (unsigned)(p[j] - '0');
    if (c > 9) return false;
    v = v * 10 + c;
  }
  if (v > (u64)INT64_MAX) return false;
  out = (i64)v;
  return true;
}

// ---------------------------------------------------------------------------
// CSV line tokenizer (rules.md dialect). Returns true iff the line has exactly
// nf well-formed fields. fs/fl receive the unquoted field contents.
static inline bool tokenize(const char* p, const char* e, int nf, const char** fs, u32* fl) {
  int k = 0;
  const char* s = p;
  for (;;) {
    if (k >= nf) return false;
    const char* q = s;
    if (q < e && *q == '"') {
      const char* c = q + 1;
      while (c < e && *c != '"' && *c != ',') c++;
      if (c >= e || *c != '"') return false;
      if (c + 1 < e && c[1] != ',') return false;
      fs[k] = q + 1;
      fl[k] = (u32)(c - q - 1);
      k++;
      q = c + 1;
    } else {
      while (q < e && *q != ',' && *q != '"') q++;
      if (q < e && *q == '"') return false;
      fs[k] = s;
      fl[k] = (u32)(q - s);
      k++;
    }
    if (q >= e) break;
    s = q + 1;
  }
  return k == nf;
}

// ---------------------------------------------------------------------------
// files

static size_t g_page = 4096;

struct MFile {
  const char* data;
  size_t size;
};

static MFile map_file(const std::string& path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    perror(path.c_str());
    exit(1);
  }
  struct stat st;
  fstat(fd, &st);
  size_t sz = (size_t)st.st_size;
  size_t mapsz = (sz + g_page - 1) / g_page * g_page + g_page;
  void* base = mmap(nullptr, mapsz, PROT_READ, MAP_PRIVATE | MAP_ANON, -1, 0);
  if (base == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  if (sz) {
    void* r = mmap(base, sz, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
    if (r == MAP_FAILED) {
      perror("mmap file");
      exit(1);
    }
#ifdef MADV_SEQUENTIAL
    madvise(base, sz, MADV_SEQUENTIAL);
#endif
  }
  close(fd);
  return {(const char*)base, sz};
}

static std::vector<std::string> list_csv(const std::string& dir) {
  std::vector<std::string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (struct dirent* e = readdir(d)) {
    const char* nm = e->d_name;
    size_t l = strlen(nm);
    if (l < 4 || nm[0] == '.') continue;
    if (strcmp(nm + l - 4, ".csv") != 0) continue;
    out.push_back(dir + "/" + nm);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

// Iterate the data lines of [b, e) of a mapped file: a line belongs to the
// chunk holding its first byte; the file's first line is the header.
template <class F>
static inline void for_lines(const MFile& f, size_t b, size_t e, F&& fn) {
  const char* base = f.data;
  const char* fend = base + f.size;
  const char* p = base + b;
  if (b == 0 || base[b - 1] != '\n') {
    const char* nl = (const char*)memchr(p, '\n', (size_t)(fend - p));
    if (!nl) return;
    p = nl + 1;
  }
  const char* cend = base + e;
  while (p < cend) {
    const char* nl = (const char*)memchr(p, '\n', (size_t)(fend - p));
    const char* le = nl ? nl : fend;
    fn(p, le);
    p = le + 1;
  }
}


// ---------------------------------------------------------------------------
// SIMD structural scanner: bitmask of ',', '"' and '\n' per 64 bytes. The
// end of the file acts as a virtual '\n'. Buffers are padded by a page, so
// reading 64 bytes at any position up to the end is safe.

#if defined(__AVX2__)
#include <immintrin.h>
static inline u64 special_mask(const char* p) {
  const __m256i c = _mm256_set1_epi8(','), q = _mm256_set1_epi8('"'), n = _mm256_set1_epi8('\n');
  __m256i a = _mm256_loadu_si256((const __m256i*)p);
  __m256i b = _mm256_loadu_si256((const __m256i*)(p + 32));
  __m256i ma = _mm256_or_si256(_mm256_or_si256(_mm256_cmpeq_epi8(a, c), _mm256_cmpeq_epi8(a, q)), _mm256_cmpeq_epi8(a, n));
  __m256i mb = _mm256_or_si256(_mm256_or_si256(_mm256_cmpeq_epi8(b, c), _mm256_cmpeq_epi8(b, q)), _mm256_cmpeq_epi8(b, n));
  return (u64)(u32)_mm256_movemask_epi8(ma) | ((u64)(u32)_mm256_movemask_epi8(mb) << 32);
}
#elif defined(__SSE2__)
#include <emmintrin.h>
static inline u64 special_mask(const char* p) {
  const __m128i c = _mm_set1_epi8(','), q = _mm_set1_epi8('"'), n = _mm_set1_epi8('\n');
  u64 m = 0;
  for (int i = 0; i < 4; i++) {
    __m128i a = _mm_loadu_si128((const __m128i*)(p + 16 * i));
    __m128i x = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(a, c), _mm_cmpeq_epi8(a, q)), _mm_cmpeq_epi8(a, n));
    m |= (u64)(u32)_mm_movemask_epi8(x) << (16 * i);
  }
  return m;
}
#elif defined(__ARM_NEON)
#include <arm_neon.h>
static inline u64 special_mask(const char* p) {
  const uint8x16_t c = vdupq_n_u8(','), q = vdupq_n_u8('"'), n = vdupq_n_u8('\n');
  static const uint8_t bits[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
  const uint8x16_t bm = vld1q_u8(bits);
  uint8x16_t m[4];
  for (int i = 0; i < 4; i++) {
    uint8x16_t a = vld1q_u8((const uint8_t*)p + 16 * i);
    m[i] = vandq_u8(vorrq_u8(vorrq_u8(vceqq_u8(a, c), vceqq_u8(a, q)), vceqq_u8(a, n)), bm);
  }
  uint8x16_t s0 = vpaddq_u8(m[0], m[1]);
  uint8x16_t s1 = vpaddq_u8(m[2], m[3]);
  s0 = vpaddq_u8(s0, s1);
  s0 = vpaddq_u8(s0, s0);
  return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
}
#else
static inline u64 special_mask(const char* p) {
  u64 m = 0;
  for (int i = 0; i < 64; i++) {
    char ch = p[i];
    if (ch == ',' || ch == '"' || ch == '\n') m |= 1ULL << i;
  }
  return m;
}
#endif

struct Scan {
  const char* blk;
  const char* fend;
  u64 bits;
  inline u64 mask_at(const char* p) const {
    u64 m = special_mask(p);
    if (UNLIKELY(p + 64 > fend)) {
      size_t r = (size_t)(fend - p);
      m &= r ? (~0ULL >> (64 - r)) : 0;
      m |= 1ULL << r;
    }
    return m;
  }
  inline void init(const char* p, const char* fe) {
    fend = fe;
    blk = p;
    bits = mask_at(p);
  }
  inline const char* next() {
    while (UNLIKELY(!bits)) {
      blk += 64;
      bits = mask_at(blk);
    }
    const char* q = blk + __builtin_ctzll(bits);
    bits &= bits - 1;
    return q;
  }
  inline char at(const char* q) const { return q == fend ? '\n' : *q; }
};

// Tokenize one line starting at s. Returns the terminator position ('\n' or
// end of file). ok tells whether the line is well-formed with NF fields.
template <int NF>
static inline const char* scan_line(Scan& sc, const char* s, const char** fs, u32* fl, bool& ok) {
  int k = 0;
  const char* q;
  char c;
  for (;;) {
    q = sc.next();
    c = sc.at(q);
    if (UNLIKELY(c == '"')) {
      if (q != s || k >= NF) goto bad;
      const char* q2 = sc.next();
      char c2 = sc.at(q2);
      if (c2 != '"') {
        q = q2;
        c = c2;
        goto bad;
      }
      const char* q3 = sc.next();
      char c3 = sc.at(q3);
      q = q3;
      c = c3;
      if (q3 != q2 + 1 || c3 == '"') goto bad;
      fs[k] = s + 1;
      fl[k] = (u32)(q2 - s - 1);
      k++;
    } else {
      if (UNLIKELY(k >= NF)) goto bad;
      fs[k] = s;
      fl[k] = (u32)(q - s);
      k++;
    }
    if (c == '\n') {
      ok = (k == NF);
      return q;
    }
    s = q + 1;
  }
bad:
  while (c != '\n') {
    q = sc.next();
    c = sc.at(q);
  }
  ok = false;
  return q;
}

template <int NF, class F>
static inline void scan_chunk(const MFile& f, size_t b, size_t e, F&& fn) {
  const char* base = f.data;
  const char* fend = base + f.size;
  const char* p = base + b;
  if (b == 0 || base[b - 1] != '\n') {
    const char* nl = (const char*)memchr(p, '\n', (size_t)(fend - p));
    if (!nl) return;
    p = nl + 1;
  }
  const char* cend = base + e;
  if (p >= cend) return;
  Scan sc;
  sc.init(p, fend);
  const char* fs[NF];
  u32 fl[NF];
  while (p < cend) {
    bool ok;
    const char* le = scan_line<NF>(sc, p, fs, fl, ok);
    if (ok) fn(fs, fl);
    p = le + 1;
  }
}

// ---------------------------------------------------------------------------
// open addressing table Key -> u32 (single writer; concurrent readers after)

struct KTable {
  struct Slot {
    u64 lo, hi;
    u32 val;
    u32 occ;
  };
  Slot* s = nullptr;
  u64 mask = 0;
  u32 count = 0;
  void init(size_t expect) {
    size_t cap = 16;
    while (cap < expect * 2) cap <<= 1;
    s = (Slot*)calloc(cap, sizeof(Slot));
    mask = cap - 1;
    count = 0;
  }
  inline const Slot* find(const Key& k) const { return find_h(k, hkey(k)); }
  inline const Slot* find_h(const Key& k, u64 h) const {
    u64 i = h & mask;
    for (;;) {
      const Slot* x = &s[i];
      if (!x->occ) return nullptr;
      if (x->lo == k.lo && x->hi == k.hi) return x;
      i = (i + 1) & mask;
    }
  }
  void grow() {
    Slot* old = s;
    u64 oc = mask + 1;
    u64 cap = oc * 2;
    s = (Slot*)calloc(cap, sizeof(Slot));
    mask = cap - 1;
    for (u64 j = 0; j < oc; j++)
      if (old[j].occ) {
        u64 i = hkey(Key{old[j].lo, old[j].hi}) & mask;
        while (s[i].occ) i = (i + 1) & mask;
        s[i] = old[j];
      }
    free(old);
  }
  // returns slot; ins tells whether it was created (val then set by caller)
  inline Slot* upsert(const Key& k, bool& ins) {
    if ((u64)(count + 1) * 2 > mask + 1) grow();
    u64 i = hkey(k) & mask;
    for (;;) {
      Slot* x = &s[i];
      if (!x->occ) {
        x->occ = 1;
        x->lo = k.lo;
        x->hi = k.hi;
        count++;
        ins = true;
        return x;
      }
      if (x->lo == k.lo && x->hi == k.hi) {
        ins = false;
        return x;
      }
      i = (i + 1) & mask;
    }
  }
};

// ---------------------------------------------------------------------------
// per-thread radix buckets: fixed-capacity regions plus overflow vectors

static void* big_alloc(size_t bytes) {
  if (bytes == 0) bytes = 1;
  void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON
#ifdef MAP_NORESERVE
                                                             | MAP_NORESERVE
#endif
                 ,
                 -1, 0);
  if (p == MAP_FAILED) {
    perror("mmap anon");
    exit(1);
  }
  return p;
}

template <class T>
struct Buckets {
  T* base = nullptr;
  u32* cnt = nullptr;
  u32 cap = 0, P = 0;
  std::vector<T>* over = nullptr;
  void init(u32 P_, u32 cap_) {
    P = P_;
    cap = cap_;
    base = (T*)big_alloc((size_t)P * cap * sizeof(T));
    cnt = (u32*)calloc(P, sizeof(u32));
    over = new std::vector<T>[P];
  }
  inline void push(u32 p, const T& v) {
    u32 c = cnt[p];
    if (LIKELY(c < cap)) {
      base[(size_t)p * cap + c] = v;
      cnt[p] = c + 1;
    } else {
      over[p].push_back(v);
    }
  }
  inline size_t size(u32 p) const { return cnt[p] + over[p].size(); }
  // give the pages back now (in parallel) instead of at process exit
  void release() {
#ifdef MADV_DONTNEED
    if (base) madvise(base, (size_t)P * cap * sizeof(T), MADV_DONTNEED);
#endif
    for (u32 p = 0; p < P; p++) std::vector<T>().swap(over[p]);
  }
  template <class F>
  inline void each(u32 p, F&& f) {
    T* b = base + (size_t)p * cap;
    for (u32 i = 0, n = cnt[p]; i < n; i++) f(b[i]);
    for (auto& v : over[p]) f(v);
  }
};

// ---------------------------------------------------------------------------
// records

struct ERec {  // parsed event copy, scattered by event_id
  u64 klo, khi;  // event_id key
  u64 ulo, uhi;  // user key; (0,0) => this copy is invalid
  i32 ingest;    // seconds since window_start (saturated)
  u32 ts;        // ms since window_start
  u32 price;     // 0 => not a purchase
  u32 cat;       // category of a purchase
};

struct CRec0 {  // valid click, per parsing thread
  u64 ulo, uhi;  // user key
  const char* cid;
  u32 cidlen;
  u32 ts;  // ms since window_start - 7 days
  u32 camp;
  u32 pad;
};

struct PRec {  // surviving event, scattered by person
  u64 plo, phi;  // person key (its name: decodable when short)
  u32 ts;
  u32 price;
  u32 cat;
  u32 pad;
};

struct CRec {  // click, scattered by person
  u64 plo, phi;
  const char* cid;
  u32 cidlen;
  u32 ts;
  u32 camp;
  u32 pad;
};

struct URow {
  Key name;
  i32 day;
  u32 sessions;
  u32 events;
  u64 revenue;
  i32 topcat;
};

struct CStat {
  u64 sessions, purch, revenue, assisted;
};

// ---------------------------------------------------------------------------
// Parquet writer (PLAIN, uncompressed, one row group, one page per column)

struct OBuf {
  std::vector<u8> b;
  inline void put(u8 x) { b.push_back(x); }
  inline void put(const void* p, size_t n) { b.insert(b.end(), (const u8*)p, (const u8*)p + n); }
  inline void varint(u64 v) {
    while (v >= 0x80) {
      put((u8)((v & 0x7F) | 0x80));
      v >>= 7;
    }
    put((u8)v);
  }
  inline void zz32(i32 v) { varint((u32)(((u32)v << 1) ^ (u32)(v >> 31))); }
  inline void zz64(i64 v) { varint(((u64)v << 1) ^ (u64)(v >> 63)); }
  inline void le32(u32 v) { put(&v, 4); }
  inline void le64(u64 v) { put(&v, 8); }
};

struct TC {
  OBuf& o;
  int last[32];
  int d = 0;
  explicit TC(OBuf& ob) : o(ob) { last[0] = 0; }
  void fh(int id, int t) {
    int delta = id - last[d];
    if (delta > 0 && delta <= 15)
      o.put((u8)((delta << 4) | t));
    else {
      o.put((u8)t);
      o.zz32(id);
    }
    last[d] = id;
  }
  void i32f(int id, i32 v) {
    fh(id, 5);
    o.zz32(v);
  }
  void i64f(int id, i64 v) {
    fh(id, 6);
    o.zz64(v);
  }
  void strf(int id, const char* s, size_t n) {
    fh(id, 8);
    o.varint(n);
    o.put(s, n);
  }
  void sbeg(int id) {
    fh(id, 12);
    last[++d] = 0;
  }
  void send() {
    o.put(0);
    d--;
  }
  void lbeg(int id, int et, int n) {
    fh(id, 9);
    if (n < 15)
      o.put((u8)((n << 4) | et));
    else {
      o.put((u8)(0xF0 | et));
      o.varint((u64)n);
    }
  }
  void ebeg() { last[++d] = 0; }
  void eend() {
    o.put(0);
    d--;
  }
  void stop() { o.put(0); }
};

enum { PT_INT32 = 1, PT_INT64 = 2, PT_BYTE_ARRAY = 6 };

struct PCol {
  const char* name;
  int ptype;
  bool optional;
  int conv;     // converted type or -1
  int logical;  // LogicalType union field id or -1
  OBuf data;    // page body (levels + values)
  i64 offset = 0, total = 0;
};

static void write_parquet(const std::string& path, i64 nrows, std::vector<PCol>& cols) {
  OBuf f;
  f.put("PAR1", 4);
  for (auto& c : cols) {
    OBuf h;
    TC t(h);
    t.i32f(1, 0);
    t.i32f(2, (i32)c.data.b.size());
    t.i32f(3, (i32)c.data.b.size());
    t.sbeg(5);
    t.i32f(1, (i32)nrows);
    t.i32f(2, 0);
    t.i32f(3, 3);
    t.i32f(4, 3);
    t.send();
    t.stop();
    c.offset = (i64)f.b.size();
    f.put(h.b.data(), h.b.size());
    f.put(c.data.b.data(), c.data.b.size());
    c.total = (i64)(h.b.size() + c.data.b.size());
  }
  OBuf m;
  TC t(m);
  t.i32f(1, 1);
  t.lbeg(2, 12, (int)cols.size() + 1);
  t.ebeg();
  t.strf(4, "schema", 6);
  t.i32f(5, (i32)cols.size());
  t.eend();
  i64 totalb = 0;
  for (auto& c : cols) {
    totalb += c.total;
    t.ebeg();
    t.i32f(1, c.ptype);
    t.i32f(3, c.optional ? 1 : 0);
    t.strf(4, c.name, strlen(c.name));
    if (c.conv >= 0) t.i32f(6, c.conv);
    if (c.logical >= 0) {
      t.sbeg(10);
      t.sbeg(c.logical);
      t.send();
      t.send();
    }
    t.eend();
  }
  t.i64f(3, nrows);
  t.lbeg(4, 12, 1);
  t.ebeg();
  t.lbeg(1, 12, (int)cols.size());
  for (auto& c : cols) {
    t.ebeg();
    t.i64f(2, c.offset);
    t.sbeg(3);
    t.i32f(1, c.ptype);
    t.lbeg(2, 5, 2);
    m.zz32(0);
    m.zz32(3);
    t.lbeg(3, 8, 1);
    m.varint(strlen(c.name));
    m.put(c.name, strlen(c.name));
    t.i32f(4, 0);
    t.i64f(5, nrows);
    t.i64f(6, c.total);
    t.i64f(7, c.total);
    t.i64f(9, c.offset);
    t.send();
    t.eend();
  }
  t.i64f(2, totalb);
  t.i64f(3, nrows);
  t.eend();
  t.stop();
  f.put(m.b.data(), m.b.size());
  f.le32((u32)m.b.size());
  f.put("PAR1", 4);

  int fd = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    perror(path.c_str());
    exit(1);
  }
  size_t off = 0;
  while (off < f.b.size()) {
    ssize_t w = write(fd, f.b.data() + off, f.b.size() - off);
    if (w <= 0) {
      perror("write");
      exit(1);
    }
    off += (size_t)w;
  }
  close(fd);
}

static inline void put_str(OBuf& o, const char* p, u32 n) {
  o.le32(n);
  o.put(p, n);
}

// definition levels (bit width 1) as one bit-packed run, with the v1 length prefix
static void put_def_levels(OBuf& o, const std::vector<u8>& defs) {
  OBuf r;
  size_t groups = (defs.size() + 7) / 8;
  if (groups) {
    r.varint((groups << 1) | 1);
    for (size_t g = 0; g < groups; g++) {
      u8 byte = 0;
      for (size_t j = 0; j < 8; j++) {
        size_t i = g * 8 + j;
        if (i < defs.size() && defs[i]) byte |= (u8)(1u << j);
      }
      r.put(byte);
    }
  }
  o.le32((u32)r.b.size());
  o.put(r.b.data(), r.b.size());
}

// ---------------------------------------------------------------------------
// global state

static std::string g_data, g_out;
static int T = 8;
static i64 g_ws, g_we, g_cs;  // window start/end, click window start (ms)
static i64 g_day0;            // epoch day of window start
static i64 g_wsmod;           // ws - day0*DAY
static int g_ndays;

static KTable g_camp;  // campaign_id -> index
static std::vector<std::string> g_camp_id, g_chan;
static u32 g_ncamp;

static KTable g_prod;  // product_id -> category
static std::vector<std::string> g_cat;  // categories, last one is "unknown"
static std::vector<u32> g_cat_rank;
static u32 g_ncat, g_unknown_cat;

// linked ids
static KTable g_link;  // id -> index
static std::vector<Key> g_link_pkey;

// hot-path map: linked user key -> person key (only ids that are not their
// person's name). 32-byte slots, key (0,0) marks empty (never a real id).
struct LSlot {
  u64 lo, hi, plo, phi;
};
static LSlot* g_lmap = nullptr;
static u64 g_lmask = 0;
static bool g_has_lmap = false;

static inline bool lmap_find(const Key& k, u64 h, Key& pk) {
  u64 i = h & g_lmask;
  for (;;) {
    const LSlot& s = g_lmap[i];
    if (s.lo == k.lo && s.hi == k.hi) {
      pk.lo = s.plo;
      pk.hi = s.phi;
      return true;
    }
    if ((s.lo | s.hi) == 0) return false;
    i = (i + 1) & g_lmask;
  }
}

static std::vector<MFile> g_ev_files, g_ck_files, g_ln_files;

struct Chunk {
  u32 file;
  u8 kind;  // 0 events, 1 clicks
  size_t b, e;
};
static std::vector<Chunk> g_chunks;
static std::atomic<u32> g_chunk_next{0};
static std::atomic<u32> g_p2_next{0};
static std::atomic<u32> g_p3_next{0};

static u32 P1, P1BITS, P2, P2BITS;

struct Barrier {
  std::atomic<int> count{0};
  std::atomic<int> gen{0};
  int n = 1;
  void wait() {
    int g = gen.load(std::memory_order_acquire);
    if (count.fetch_add(1, std::memory_order_acq_rel) == n - 1) {
      count.store(0, std::memory_order_relaxed);
      gen.fetch_add(1, std::memory_order_release);
    } else {
      int spins = 0;
      while (gen.load(std::memory_order_acquire) == g) {
        if (++spins > 2000) sched_yield();
      }
    }
  }
};
static Barrier g_bar;

#include <chrono>
static bool g_timing = false;
static std::chrono::steady_clock::time_point g_t0;
static void tmark(const char* what) {
  if (!g_timing) return;
  double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - g_t0).count();
  fprintf(stderr, "%8.2f ms  %s\n", ms, what);
}

struct NameRef {
  const char* p;
  u32 n;
};

static std::vector<NameRef> g_link_long;  // long person names from links

struct TState {
  std::vector<NameRef> longnames;  // user ids over 15 bytes seen in valid events
  Buckets<ERec> ebk;
  std::vector<CRec0> clicks;
  Buckets<PRec> pbk;
  Buckets<CRec> cbk;
  std::vector<URow> urows;
  std::vector<CStat> cstat;
};
static TState* g_ts;

// ---------------------------------------------------------------------------
// small tables built at start

static std::string read_small(const std::string& path) {
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) {
    perror(path.c_str());
    exit(1);
  }
  std::string s;
  char buf[65536];
  size_t n;
  while ((n = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, n);
  fclose(f);
  return s;
}

static i64 manifest_ts(const std::string& js, const char* name) {
  size_t k = js.find(name);
  if (k == std::string::npos) {
    fprintf(stderr, "manifest: missing %s\n", name);
    exit(1);
  }
  size_t c = js.find(':', k + strlen(name));
  size_t q1 = js.find('"', c + 1);
  size_t q2 = js.find('"', q1 + 1);
  std::string v = js.substr(q1 + 1, q2 - q1 - 1);
  v.append(32, '\0');
  i64 ms;
  if (!parse_ts(v.data(), (u32)(q2 - q1 - 1), ms)) {
    fprintf(stderr, "manifest: bad %s\n", name);
    exit(1);
  }
  return ms;
}

static void build_campaigns() {
  MFile f = map_file(g_data + "/campaigns.csv");
  g_camp.init(4096);
  for_lines(f, 0, f.size, [&](const char* p, const char* e) {
    const char* fs[2];
    u32 fl[2];
    if (!tokenize(p, e, 2, fs, fl) || fl[0] == 0) return;
    bool ins;
    auto* s = g_camp.upsert(make_key(fs[0], fl[0]), ins);
    if (ins) {
      s->val = (u32)g_camp_id.size();
      g_camp_id.emplace_back(fs[0], fl[0]);
      g_chan.emplace_back(fs[1], fl[1]);
    }
  });
  g_ncamp = (u32)g_camp_id.size();
}

// Products are built by all threads together before parsing: each takes a
// line-aligned slice and inserts lock-free (product ids are unique; a slot is
// claimed by CAS on its occupied flag and only read after the barrier).
// Category 0 is "unknown"; others are interned under a small mutex.
static MFile g_prod_file;
static std::mutex g_cat_mu;

static void build_products_init() {
  g_prod_file = map_file(g_data + "/products.csv");
  size_t lines = 1;
  const char* p = g_prod_file.data;
  const char* e = p + g_prod_file.size;
  while ((p = (const char*)memchr(p, '\n', (size_t)(e - p))) != nullptr) {
    lines++;
    p++;
  }
  g_prod.init(lines + 16);
  g_cat.push_back("unknown");
  g_unknown_cat = 0;
}

static void build_products_part(int tid) {
  const MFile& f = g_prod_file;
  size_t b = f.size * (size_t)tid / (size_t)T, e = f.size * (size_t)(tid + 1) / (size_t)T;
  if (b >= e) return;
  std::vector<std::pair<Key, u32>> local;
  for_lines(f, b, e, [&](const char* p, const char* le) {
    const char* fs[2];
    u32 fl[2];
    if (!tokenize(p, le, 2, fs, fl) || fl[0] == 0) return;
    Key ck = make_key(fs[1], fl[1]);
    u32 ci = 0;
    bool found = false;
    for (auto& x : local)
      if (key_eq(x.first, ck)) {
        ci = x.second;
        found = true;
        break;
      }
    if (!found) {
      std::lock_guard<std::mutex> g(g_cat_mu);
      std::string nm(fs[1], fl[1]);
      u32 i = 1;
      for (; i < g_cat.size(); i++)
        if (g_cat[i] == nm) break;
      if (i == g_cat.size()) g_cat.push_back(nm);
      ci = i;
      local.push_back({ck, ci});
    }
    Key k = make_key(fs[0], fl[0]);
    u64 i = hkey(k) & g_prod.mask;
    for (;;) {
      KTable::Slot* x = &g_prod.s[i];
      u32 z = 0;
      if (__atomic_compare_exchange_n(&x->occ, &z, 1u, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
        x->lo = k.lo;
        x->hi = k.hi;
        x->val = ci;
        break;
      }
      i = (i + 1) & g_prod.mask;
    }
  });
}

static void build_category_ranks() {
  g_ncat = (u32)g_cat.size();
  std::vector<u32> order(g_cat.size());
  for (u32 i = 0; i < order.size(); i++) order[i] = i;
  std::sort(order.begin(), order.end(), [](u32 x, u32 y) { return g_cat[x] < g_cat[y]; });
  g_cat_rank.assign(g_cat.size(), 0);
  for (u32 r = 0; r < order.size(); r++) g_cat_rank[order[r]] = r;
}

static void build_links() {
  struct Id {
    const char* p;
    u32 n;
  };
  std::vector<Id> ids;
  std::vector<u32> parent;
  size_t bytes = 0;
  for (auto& f : g_ln_files) bytes += f.size;
  g_link.init(bytes / 12 + 64);
  ids.reserve(bytes / 12 + 64);
  parent.reserve(bytes / 12 + 64);
  auto find = [&](u32 x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  };
  auto intern = [&](const char* p, u32 n) -> u32 {
    bool ins;
    auto* s = g_link.upsert(make_key(p, n), ins);
    if (ins) {
      s->val = (u32)ids.size();
      ids.push_back({p, n});
      parent.push_back(s->val);
    }
    return s->val;
  };
  for (auto& f : g_ln_files) {
    for_lines(f, 0, f.size, [&](const char* p, const char* e) {
      const char* fs[2];
      u32 fl[2];
      if (!tokenize(p, e, 2, fs, fl) || fl[0] == 0 || fl[1] == 0) return;
      u32 a = intern(fs[0], fl[0]);
      u32 b = intern(fs[1], fl[1]);
      a = find(a);
      b = find(b);
      if (a != b) {
        if (a < b)
          parent[b] = a;
        else
          parent[a] = b;
      }
    });
  }
  size_t n = ids.size();
  std::vector<u32> best(n);
  for (size_t i = 0; i < n; i++) best[i] = (u32)i;
  for (size_t i = 0; i < n; i++) {
    u32 r = find((u32)i);
    u32 b = best[r];
    if (str_less(ids[i].p, ids[i].n, ids[b].p, ids[b].n)) best[r] = (u32)i;
  }
  g_link_pkey.resize(n);
  for (size_t i = 0; i < n; i++) {
    u32 b = best[find((u32)i)];
    g_link_pkey[i] = make_key(ids[b].p, ids[b].n);
    if (b == i && ids[b].n > 15) g_link_long.push_back(NameRef{ids[b].p, ids[b].n});
  }
  size_t nm = 0;
  for (size_t i = 0; i < n; i++)
    if (best[find((u32)i)] != i) nm++;
  if (nm) {
    size_t cap = 16;
    while (cap < nm * 2) cap <<= 1;
    void* mem = nullptr;
    if (posix_memalign(&mem, 64, cap * sizeof(LSlot)) != 0) exit(1);
    memset(mem, 0, cap * sizeof(LSlot));
    g_lmap = (LSlot*)mem;
    g_lmask = cap - 1;
    for (size_t i = 0; i < n; i++) {
      if (best[find((u32)i)] == i) continue;
      Key k = make_key(ids[i].p, ids[i].n);
      u64 j = hkey(k) & g_lmask;
      while ((g_lmap[j].lo | g_lmap[j].hi) != 0) j = (j + 1) & g_lmask;
      g_lmap[j] = LSlot{k.lo, k.hi, g_link_pkey[i].lo, g_link_pkey[i].hi};
    }
    g_has_lmap = true;
  }
}

// ---------------------------------------------------------------------------
// names: ids up to 15 bytes are stored inside their key; longer ones (never
// produced by the generator, but legal) are kept in a registry.

static KTable g_long;
static std::vector<NameRef> g_long_ref;

static void build_long_names() {
  size_t n = g_link_long.size();
  for (int t = 0; t < T; t++) n += g_ts[t].longnames.size();
  g_long.init(n + 16);
  auto add = [&](const NameRef& r) {
    bool ins;
    auto* s = g_long.upsert(make_key(r.p, r.n), ins);
    if (ins) {
      s->val = (u32)g_long_ref.size();
      g_long_ref.push_back(r);
    }
  };
  for (auto& r : g_link_long) add(r);
  for (int t = 0; t < T; t++)
    for (auto& r : g_ts[t].longnames) add(r);
}

static inline NameRef key_name(const Key& k, char* buf16) {
  u32 n = (u32)(k.hi >> 56);
  if (n <= 15) {
    memcpy(buf16, &k.lo, 8);
    memcpy(buf16 + 8, &k.hi, 8);
    return NameRef{buf16, n};
  }
  return g_long_ref[g_long.find(k)->val];
}


// ---------------------------------------------------------------------------
// phase 1: parsing

static inline void parse_event_line(TState& ts, const char** fs, const u32* fl) {
  if (UNLIKELY(fl[0] == 0)) return;
  ERec r;
  Key k = make_key(fs[0], fl[0]);
  r.klo = k.lo;
  r.khi = k.hi;
  i64 ing;
  if (UNLIKELY(!parse_ts(fs[3], fl[3], ing))) ing = INT64_MAX / 4;
  {
    i64 rs = ing / 1000 - g_ws / 1000;
    r.ingest = rs > INT32_MAX ? INT32_MAX : (rs < INT32_MIN ? INT32_MIN : (i32)rs);
  }
  r.ulo = 0;
  r.uhi = 0;
  r.ts = 0;
  r.price = 0;
  r.cat = 0;
  bool ok = fl[1] != 0 && fl[5] != 0;
  i64 t = 0;
  if (ok) ok = parse_ts(fs[2], fl[2], t) && t >= g_ws && t < g_we;
  u32 price = 0;
  if (ok) {
    const char* ty = fs[4];
    u32 tl = fl[4];
    if (tl == 4 && (memcmp(ty, "view", 4) == 0 || memcmp(ty, "cart", 4) == 0)) {
    } else if (tl == 8 && memcmp(ty, "purchase", 8) == 0) {
      const char* pp = fs[6];
      u32 pl = fl[6];
      u32 i = 0;
      while (i < pl && pp[i] == '0') i++;
      if (pl == 0 || pl - i > 8) {
        ok = false;
      } else {
        u64 v = 0;
        for (u32 j = 0; j < pl; j++) {
          char c = pp[j];
          if (!isd(c)) {
            ok = false;
            break;
          }
          v = v * 10 + (u64)(c - '0');
        }
        if (ok && (v < 1 || v > 10000000)) ok = false;
        price = (u32)v;
      }
    } else {
      ok = false;
    }
  }
  if (ok && ing - t > DAY_MS) ok = false;
  if (ok) {
    Key uk = make_key(fs[1], fl[1]);
    r.ulo = uk.lo;
    r.uhi = uk.hi;
    if (UNLIKELY(fl[1] > 15)) ts.longnames.push_back({fs[1], fl[1]});
    r.ts = (u32)(t - g_ws);
    r.price = price;
    if (price) {
      const KTable::Slot* s = g_prod.find(make_key(fs[5], fl[5]));
      r.cat = s ? s->val : g_unknown_cat;
    }
  }
  ts.ebk.push((u32)(hkey(k) >> (64 - P1BITS)), r);
}

static inline void parse_click_line(TState& ts, const char** fs, const u32* fl) {
  if (fl[0] == 0 || fl[1] == 0) return;
  i64 t;
  if (!parse_ts(fs[3], fl[3], t) || t < g_cs || t >= g_we) return;
  const KTable::Slot* c = g_camp.find(make_key(fs[2], fl[2]));
  if (!c) return;
  CRec0 r;
  Key uk = make_key(fs[1], fl[1]);
  r.ulo = uk.lo;
  r.uhi = uk.hi;
  r.pad = 0;
  r.cid = fs[0];
  r.cidlen = fl[0];
  r.ts = (u32)(t - g_cs);
  r.camp = c->val;
  ts.clicks.push_back(r);
}

// ---------------------------------------------------------------------------
// person mapping

// ---------------------------------------------------------------------------
// phase 3 scratch

struct Ev {
  u32 ts, price, cat;
};
struct Cl {
  u32 ts, camp;
  const char* cid;
  u32 cidlen;
};

struct PInfo {
  Key name;
  u32 ecnt, eoff;
  u32 ccnt, coff;
};

static inline bool cl_less(const Cl& a, const Cl& b) {
  if (a.ts != b.ts) return a.ts < b.ts;
  return str_less(b.cid, b.cidlen, a.cid, a.cidlen);  // click_id descending
}

struct Scratch {
  std::vector<u32> ptab;  // person table: index+1, 0 empty
  std::vector<Key> pkeys;
  std::vector<PInfo> pinfo;
  std::vector<u32> evp;
  std::vector<Ev> ev;
  std::vector<PRec*> prs;
  std::vector<u32> clp;
  std::vector<CRec*> crs;
  std::vector<Cl> cl;
  std::vector<u32> ccnt, cpos, active;
  std::vector<u64> catrev;
  std::vector<u32> cattouch;
};

static void process_person(TState& ts, Scratch& sc, const PInfo& pi) {
  Ev* ev = sc.ev.data() + pi.eoff;
  u32 n = pi.ecnt;
  if (n > 1) {
    if (n <= 24) {
      for (u32 i = 1; i < n; i++) {
        Ev x = ev[i];
        u32 j = i;
        while (j > 0 && ev[j - 1].ts > x.ts) {
          ev[j] = ev[j - 1];
          j--;
        }
        ev[j] = x;
      }
    } else {
      std::sort(ev, ev + n, [](const Ev& a, const Ev& b) { return a.ts < b.ts; });
    }
  }
  Cl* cl = sc.cl.data() + pi.coff;
  u32 m = pi.ccnt;
  if (m > 1) std::sort(cl, cl + m, cl_less);

  CStat* cs = ts.cstat.data();
  const u32 nd = (u32)g_ndays;
  const u32 organic = g_ncamp;
  u32* ccnt = sc.ccnt.data();
  u32* cpos = sc.cpos.data();
  std::vector<u32>& active = sc.active;
  u64* catrev = sc.catrev.data();
  std::vector<u32>& touch = sc.cattouch;

  u32 lo = 0, hi = 0;
  i32 cur_day = -1;
  u32 d_sessions = 0, d_events = 0;
  u64 d_rev = 0;

  auto flush = [&]() {
    i32 top = -1;
    if (!touch.empty()) {
      u64 best = 0;
      u32 bc = 0;
      bool have = false;
      for (u32 c : touch) {
        u64 v = catrev[c];
        if (!have || v > best || (v == best && g_cat_rank[c] < g_cat_rank[bc])) {
          best = v;
          bc = c;
          have = true;
        }
        catrev[c] = 0;
      }
      touch.clear();
      top = (i32)bc;
    }
    ts.urows.push_back(URow{pi.name, (i32)(g_day0 + cur_day), d_sessions, d_events, d_rev, top});
  };

  u32 i = 0;
  while (i < n) {
    u32 start = ev[i].ts;
    i32 day = (i32)(((i64)start + g_wsmod) / DAY_MS);
    if (day != cur_day) {
      if (cur_day >= 0) flush();
      cur_day = day;
      d_sessions = d_events = 0;
      d_rev = 0;
    }
    u32 prev = start;
    u32 j = i;
    u64 rev = 0;
    u32 npur = 0;
    do {
      const Ev& x = ev[j];
      if (x.price) {
        rev += x.price;
        npur++;
        if (catrev[x.cat] == 0) touch.push_back(x.cat);
        catrev[x.cat] += x.price;
      }
      prev = x.ts;
      j++;
    } while (j < n && (u64)(ev[j].ts - prev) <= (u64)GAP_MS);
    d_sessions++;
    d_events += j - i;
    d_rev += rev;

    // attribution: in click time (ms since ws - 7d), qualifying clicks have
    // start <= click.ts <= start + 7d
    u32 cid_credit = organic;
    if (m) {
      u64 S = (u64)start + (u64)WEEK_MS;
      while (hi < m && cl[hi].ts <= S) {
        u32 c = cl[hi].camp;
        if (ccnt[c]++ == 0) {
          cpos[c] = (u32)active.size();
          active.push_back(c);
        }
        hi++;
      }
      while (lo < hi && cl[lo].ts < start) {
        u32 c = cl[lo].camp;
        if (--ccnt[c] == 0) {
          u32 last = active.back();
          active[cpos[c]] = last;
          cpos[last] = cpos[c];
          active.pop_back();
        }
        lo++;
      }
      if (hi > lo) cid_credit = cl[hi - 1].camp;
      for (u32 a : active) cs[(size_t)a * nd + day].assisted++;
    }
    CStat& st = cs[(size_t)cid_credit * nd + day];
    st.sessions++;
    st.purch += npur ? 1 : 0;
    st.revenue += rev;
    i = j;
  }
  if (cur_day >= 0) flush();
  for (u32 a : active) ccnt[a] = 0;
  active.clear();
}

static void process_partition(TState& ts, Scratch& sc, u32 p) {
  // gather events
  sc.prs.clear();
  for (int t = 0; t < T; t++) g_ts[t].pbk.each(p, [&](PRec& r) { sc.prs.push_back(&r); });
  u32 n = (u32)sc.prs.size();
  if (n == 0) return;
  u32 cap = 16;
  while (cap < n * 2) cap <<= 1;
  if (sc.ptab.size() < cap) sc.ptab.resize(cap);
  std::fill(sc.ptab.begin(), sc.ptab.begin() + cap, 0u);
  u32 mask = cap - 1;
  sc.pkeys.clear();
  sc.pinfo.clear();
  sc.evp.resize(n);
  for (u32 i = 0; i < n; i++) {
    PRec& r = *sc.prs[i];
    Key k{r.plo, r.phi};
    u32 h = (u32)hkey(k) & mask;
    u32 idx;
    for (;;) {
      u32 v = sc.ptab[h];
      if (v == 0) {
        idx = (u32)sc.pkeys.size();
        sc.ptab[h] = idx + 1;
        sc.pkeys.push_back(k);
        sc.pinfo.push_back(PInfo{k, 0, 0, 0, 0});
        break;
      }
      if (key_eq(sc.pkeys[v - 1], k)) {
        idx = v - 1;
        break;
      }
      h = (h + 1) & mask;
    }
    sc.evp[i] = idx;
    sc.pinfo[idx].ecnt++;
  }
  u32 np = (u32)sc.pinfo.size();
  u32 off = 0;
  for (u32 q = 0; q < np; q++) {
    sc.pinfo[q].eoff = off;
    off += sc.pinfo[q].ecnt;
    sc.pinfo[q].ecnt = 0;
  }
  sc.ev.resize(n);
  for (u32 i = 0; i < n; i++) {
    PInfo& pi = sc.pinfo[sc.evp[i]];
    PRec& r = *sc.prs[i];
    sc.ev[pi.eoff + pi.ecnt++] = Ev{r.ts, r.price, r.cat};
  }
  // clicks of persons with events
  sc.crs.clear();
  sc.clp.clear();
  for (int t = 0; t < T; t++)
    g_ts[t].cbk.each(p, [&](CRec& r) {
      Key k{r.plo, r.phi};
      u32 h = (u32)hkey(k) & mask;
      for (;;) {
        u32 v = sc.ptab[h];
        if (v == 0) return;
        if (key_eq(sc.pkeys[v - 1], k)) {
          sc.crs.push_back(&r);
          sc.clp.push_back(v - 1);
          sc.pinfo[v - 1].ccnt++;
          return;
        }
        h = (h + 1) & mask;
      }
    });
  u32 mc = (u32)sc.crs.size();
  off = 0;
  for (u32 q = 0; q < np; q++) {
    sc.pinfo[q].coff = off;
    off += sc.pinfo[q].ccnt;
    sc.pinfo[q].ccnt = 0;
  }
  sc.cl.resize(mc);
  for (u32 i = 0; i < mc; i++) {
    PInfo& pi = sc.pinfo[sc.clp[i]];
    CRec& r = *sc.crs[i];
    sc.cl[pi.coff + pi.ccnt++] = Cl{r.ts, r.camp, r.cid, r.cidlen};
  }
  for (u32 q = 0; q < np; q++) process_person(ts, sc, sc.pinfo[q]);
}

static void write_user_part(TState& ts, int tid) {
  std::vector<PCol> cols(6);
  cols[0] = PCol{"user_id", PT_BYTE_ARRAY, false, 0, 1, {}};
  cols[1] = PCol{"day", PT_INT32, false, 6, 6, {}};
  cols[2] = PCol{"sessions", PT_INT64, false, -1, -1, {}};
  cols[3] = PCol{"events", PT_INT64, false, -1, -1, {}};
  cols[4] = PCol{"revenue_cents", PT_INT64, false, -1, -1, {}};
  cols[5] = PCol{"top_category", PT_BYTE_ARRAY, true, 0, 1, {}};
  size_t n = ts.urows.size();
  size_t namebytes = 0;
  namebytes = n * 20;
  cols[0].data.b.reserve(namebytes);
  cols[1].data.b.reserve(n * 4);
  for (int c = 2; c < 5; c++) cols[c].data.b.reserve(n * 8);
  std::vector<u8> defs(n);
  OBuf topv;
  for (size_t i = 0; i < n; i++) {
    const URow& r = ts.urows[i];
    char nb[16];
    NameRef nm = key_name(r.name, nb);
    put_str(cols[0].data, nm.p, nm.n);
    cols[1].data.le32((u32)r.day);
    cols[2].data.le64(r.sessions);
    cols[3].data.le64(r.events);
    cols[4].data.le64(r.revenue);
    if (r.topcat >= 0) {
      defs[i] = 1;
      const std::string& s = g_cat[(size_t)r.topcat];
      put_str(topv, s.data(), (u32)s.size());
    }
  }
  put_def_levels(cols[5].data, defs);
  cols[5].data.put(topv.b.data(), topv.b.size());
  char name[64];
  snprintf(name, sizeof name, "/user_daily/part-%03d.parquet", tid);
  write_parquet(g_out + name, (i64)n, cols);
}

static void write_campaign() {
  size_t cells = (size_t)(g_ncamp + 1) * g_ndays;
  std::vector<CStat> tot(cells);
  for (int t = 0; t < T; t++) {
    const CStat* s = g_ts[t].cstat.data();
    for (size_t i = 0; i < cells; i++) {
      tot[i].sessions += s[i].sessions;
      tot[i].purch += s[i].purch;
      tot[i].revenue += s[i].revenue;
      tot[i].assisted += s[i].assisted;
    }
  }
  std::vector<PCol> cols(7);
  cols[0] = PCol{"campaign_id", PT_BYTE_ARRAY, false, 0, 1, {}};
  cols[1] = PCol{"channel", PT_BYTE_ARRAY, false, 0, 1, {}};
  cols[2] = PCol{"day", PT_INT32, false, 6, 6, {}};
  cols[3] = PCol{"sessions", PT_INT64, false, -1, -1, {}};
  cols[4] = PCol{"purchasing_sessions", PT_INT64, false, -1, -1, {}};
  cols[5] = PCol{"revenue_cents", PT_INT64, false, -1, -1, {}};
  cols[6] = PCol{"assisted_sessions", PT_INT64, false, -1, -1, {}};
  i64 n = 0;
  for (u32 c = 0; c <= g_ncamp; c++) {
    for (int d = 0; d < g_ndays; d++) {
      const CStat& s = tot[(size_t)c * g_ndays + d];
      if (s.sessions == 0 && s.assisted == 0) continue;
      n++;
      if (c == g_ncamp) {
        put_str(cols[0].data, "organic", 7);
        put_str(cols[1].data, "organic", 7);
      } else {
        put_str(cols[0].data, g_camp_id[c].data(), (u32)g_camp_id[c].size());
        put_str(cols[1].data, g_chan[c].data(), (u32)g_chan[c].size());
      }
      cols[2].data.le32((u32)(g_day0 + d));
      cols[3].data.le64(s.sessions);
      cols[4].data.le64(s.purch);
      cols[5].data.le64(s.revenue);
      cols[6].data.le64(s.assisted);
    }
  }
  write_parquet(g_out + "/campaign_daily/part-000.parquet", n, cols);
}

// ---------------------------------------------------------------------------
// worker

static u32 g_e_cap, g_c_cap, g_p_cap;
static size_t g_click_reserve = 1024;

static void worker(int tid) {
  TState& ts = g_ts[tid];
  ts.ebk.init(P1, g_e_cap);
  ts.clicks.reserve(g_click_reserve);

  // phase 0: products (shared by all threads)
  build_products_part(tid);
  g_bar.wait();

  // phase 1
  if (tid == 0) build_links();

  for (;;) {
    u32 ci = g_chunk_next.fetch_add(1, std::memory_order_relaxed);
    if (ci >= g_chunks.size()) break;
    const Chunk& c = g_chunks[ci];
    if (c.kind == 0) {
      const MFile& f = g_ev_files[c.file];
      scan_chunk<7>(f, c.b, c.e, [&](const char** fs, const u32* fl) { parse_event_line(ts, fs, fl); });
    } else {
      const MFile& f = g_ck_files[c.file];
      scan_chunk<4>(f, c.b, c.e, [&](const char** fs, const u32* fl) { parse_click_line(ts, fs, fl); });
    }
  }
  ts.pbk.init(P2, g_p_cap);
  ts.cbk.init(P2, g_c_cap);
  g_bar.wait();
  if (tid == 0) tmark("phase1 parse");

  // phase 2: dedup by event_id partition
  if (tid == 0) {
    build_long_names();
    build_category_ranks();
  }
  {
    std::vector<ERec*> tab;
    std::vector<ERec*> win;
    enum { BATCH = 32 };
    u64 bh[BATCH];
    const bool links = g_has_lmap;
    for (;;) {
      u32 p = g_p2_next.fetch_add(1, std::memory_order_relaxed);
      if (p >= P1) break;
      size_t n = 0;
      for (int t = 0; t < T; t++) n += g_ts[t].ebk.size(p);
      if (!n) continue;
      size_t cap = 16;
      while (cap < n * 2) cap <<= 1;
      if (tab.size() < cap) tab.resize(cap);
      std::fill(tab.begin(), tab.begin() + cap, nullptr);
      u64 mask = cap - 1;
      for (int t = 0; t < T; t++)
        g_ts[t].ebk.each(p, [&](ERec& r) {
          u64 h = hkey(Key{r.klo, r.khi}) & mask;
          for (;;) {
            ERec* s = tab[h];
            if (!s) {
              tab[h] = &r;
              return;
            }
            if (s->klo == r.klo && s->khi == r.khi) {
              if (r.ingest < s->ingest) tab[h] = &r;
              return;
            }
            h = (h + 1) & mask;
          }
        });
      // valid winners: map user -> person in prefetched batches, scatter
      win.clear();
      for (size_t i = 0; i < cap; i++) {
        ERec* r = tab[i];
        if (r && (r->ulo | r->uhi)) win.push_back(r);
      }
      const size_t nw = win.size();
      for (size_t b = 0; b < nw; b += BATCH) {
        size_t e = std::min(nw, b + BATCH);
        for (size_t k = b; k < e; k++) {
          u64 h = hkey(Key{win[k]->ulo, win[k]->uhi});
          bh[k - b] = h;
          if (links) __builtin_prefetch(&g_lmap[h & g_lmask]);
        }
        for (size_t k = b; k < e; k++) {
          const ERec* r = win[k];
          Key pk{r->ulo, r->uhi};
          u64 ph = bh[k - b];
          if (links && lmap_find(pk, ph, pk)) ph = hkey(pk);
          ts.pbk.push((u32)(ph >> (64 - P2BITS)), PRec{pk.lo, pk.hi, r->ts, r->price, r->cat, 0});
        }
      }
    }
    {
      const size_t nc = ts.clicks.size();
      for (size_t b = 0; b < nc; b += BATCH) {
        size_t e = std::min(nc, b + BATCH);
        for (size_t k = b; k < e; k++) {
          u64 h = hkey(Key{ts.clicks[k].ulo, ts.clicks[k].uhi});
          bh[k - b] = h;
          if (links) __builtin_prefetch(&g_lmap[h & g_lmask]);
        }
        for (size_t k = b; k < e; k++) {
          const CRec0& c = ts.clicks[k];
          Key pk{c.ulo, c.uhi};
          u64 ph = bh[k - b];
          if (links && lmap_find(pk, ph, pk)) ph = hkey(pk);
          ts.cbk.push((u32)(ph >> (64 - P2BITS)), CRec{pk.lo, pk.hi, c.cid, c.cidlen, c.ts, c.camp, 0});
        }
      }
    }
  }
  g_bar.wait();
  if (tid == 0) tmark("phase2 dedup+scatter");
  ts.ebk.release();
  std::vector<CRec0>().swap(ts.clicks);

  // phase 3: per person partition
  {
    Scratch sc;
    sc.ccnt.assign(g_ncamp + 1, 0);
    sc.cpos.assign(g_ncamp + 1, 0);
    sc.catrev.assign(g_cat.size(), 0);
    ts.cstat.assign((size_t)(g_ncamp + 1) * g_ndays, CStat{0, 0, 0, 0});
    // largest partitions first
    std::vector<std::pair<u32, u32>> order(P2);
    for (u32 p = 0; p < P2; p++) {
      size_t s = 0;
      for (int t = 0; t < T; t++) s += g_ts[t].pbk.size(p);
      order[p] = {(u32)s, p};
    }
    std::sort(order.begin(), order.end(), [](const std::pair<u32, u32>& a, const std::pair<u32, u32>& b) {
      return a.first > b.first || (a.first == b.first && a.second < b.second);
    });
    for (;;) {
      u32 k = g_p3_next.fetch_add(1, std::memory_order_relaxed);
      if (k >= P2) break;
      process_partition(ts, sc, order[k].second);
    }
  }
  if (g_timing) {
    char b[64];
    snprintf(b, sizeof b, "phase3 done t%d", tid);
    tmark(b);
  }
  g_bar.wait();
  if (tid == 0) write_campaign();
  if (tid == 0 || !ts.urows.empty()) write_user_part(ts, tid);
}

// ---------------------------------------------------------------------------

static u32 pow2_bits(u64 x) {
  u32 b = 0;
  while ((1ULL << b) < x) b++;
  return b;
}

int main() {
  g_t0 = std::chrono::steady_clock::now();
  g_timing = getenv("SS_TIMING") != nullptr;
  g_page = (size_t)sysconf(_SC_PAGESIZE);
  const char* dd = getenv("DATA_DIR");
  const char* od = getenv("OUTPUT_DIR");
  g_data = dd ? dd : "/data";
  g_out = od ? od : "/output";
  const char* nc = getenv("NCPU");
  T = nc ? atoi(nc) : (int)std::thread::hardware_concurrency();
  if (T < 1) T = 1;
  if (T > 64) T = 64;

  std::string mf = read_small(g_data + "/manifest.json");
  g_ws = manifest_ts(mf, "\"window_start\"");
  g_we = manifest_ts(mf, "\"window_end\"");
  g_cs = g_ws - WEEK_MS;
  g_day0 = (g_ws >= 0 ? g_ws / DAY_MS : (g_ws - DAY_MS + 1) / DAY_MS);
  g_wsmod = g_ws - g_day0 * DAY_MS;
  g_ndays = (int)((g_we - 1 - g_day0 * DAY_MS) / DAY_MS) + 1;
  if (g_ndays < 1) g_ndays = 1;

  build_campaigns();
  build_products_init();

  std::vector<std::string> evn = list_csv(g_data + "/events");
  std::vector<std::string> ckn = list_csv(g_data + "/clicks");
  std::vector<std::string> lnn = list_csv(g_data + "/links");
  size_t ebytes = 0, cbytes = 0;
  for (auto& s : evn) {
    g_ev_files.push_back(map_file(s));
    ebytes += g_ev_files.back().size;
  }
  for (auto& s : ckn) {
    g_ck_files.push_back(map_file(s));
    cbytes += g_ck_files.back().size;
  }
  for (auto& s : lnn) g_ln_files.push_back(map_file(s));

  const size_t CH = 1 << 20;
  for (u32 i = 0; i < g_ck_files.size(); i++) {
    size_t sz = g_ck_files[i].size;
    for (size_t b = 0; b < sz; b += CH) g_chunks.push_back(Chunk{i, 1, b, std::min(sz, b + CH)});
  }
  for (u32 i = 0; i < g_ev_files.size(); i++) {
    size_t sz = g_ev_files[i].size;
    for (size_t b = 0; b < sz; b += CH) g_chunks.push_back(Chunk{i, 0, b, std::min(sz, b + CH)});
  }
  // big chunks interleaved: keep file order but events first is fine

  u64 est_events = ebytes / 60 + 1024;
  u64 est_clicks = cbytes / 40 + 1024;
  u64 target = std::max<u64>(est_events / 4096, 64);
  P1BITS = pow2_bits(std::min<u64>(target, 1024));
  P1 = 1u << P1BITS;
  P2BITS = P1BITS;
  P2 = P1;
  g_e_cap = (u32)(est_events * 3 / 2 / ((u64)T * P1) + 64);
  g_p_cap = g_e_cap;
  g_c_cap = (u32)(est_clicks * 3 / 2 / ((u64)T * P2) + 64);
  g_click_reserve = (size_t)(est_clicks * 3 / 2 / (u64)T + 1024);

  mkdir((g_out + "/user_daily").c_str(), 0755);
  mkdir((g_out + "/campaign_daily").c_str(), 0755);

  tmark("setup");
  g_bar.n = T;
  g_ts = new TState[T];
  std::vector<std::thread> th;
  for (int t = 1; t < T; t++) th.emplace_back(worker, t);
  worker(0);
  for (auto& x : th) x.join();
  tmark("end");
  return 0;
}
