class Murmur2 {
  static const int SEED = 0x9747b28c;
  static const int M = 0x5bd1e995;
  static const int R = 24;

  static int eval(List<int> data) {
    final int length = data.length;
    int h = SEED ^ length;

    for (int i = 0; i < length - 3; i += 4) {
      int k =
          (data[i + 0] & 0xff) +
              ((data[i + 1] & 0xff) << 8) +
              ((data[i + 2] & 0xff) << 16) +
              ((data[i + 3] & 0xff) << 24);

      k = (k * M) & 0xffffffff;
      k ^= k >>> R;
      k = (k * M) & 0xffffffff;

      h = (h * M) & 0xffffffff;
      h ^= k;
    }

    var tail = length % 4;
    if (tail > 2) {
      h ^= (data[(length & ~3) + 2] & 0xff) << 16;
    }
    if (tail > 1) {
      h ^= (data[(length & ~3) + 1] & 0xff) << 8;
    }
    if (tail > 0) {
      h ^= data[length & ~3] & 0xff;
      h = (h * M) & 0xffffffff;
    }

    h ^= h >>> 13;
    h = (h * M) & 0xffffffff;
    h ^= h >>> 15;

    return (h & 0x80000000 == 0) ? h : ~((~h & 0x7FFFFFFF));
  }
}