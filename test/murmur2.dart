import 'package:kafkabr/src/util/murmur2.dart';
import 'package:test/test.dart';

void main() {
  const testData = {
    '21': -973932308,
    'foobar': -790332482,
    'a-little-bit-long-string': -985981536,
    'a-little-bit-longer-string': -1486304829,
    'lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8': -58897971,
    'abc': 479470107,
    'string-f-24-chars-length': 124730809,
    'string-fo-25-chars-length': 1269511650,
    'string-foo-26-chars-length': 119165628,
    'string-last-27-chars-length': 1089971754,
    // Cases from librdkafka
    // https://github.com/confluentinc/librdkafka/blob/master/src/rdmurmur2.c
    'kafka': -798503068, //0xd067cf64,
    'giberish123456789': -1890243828, //0x8f552b0c,
    '1234': -1614185708, //0x9fc97b14,
    '234': -406844982, //0xe7c009ca,
    '34': -2026295078, //0x873930da,
    '4': 1514888353, //0x5a4b5ca1,
    'PreAmbleWillBeRemoved,ThePrePartThatIs': 2017611548, //0x78424f1c,
    'reAmbleWillBeRemoved,ThePrePartThatIs': 1247982455, //0x4a62b377,
    'eAmbleWillBeRemoved,ThePrePartThatIs': -521871202, //0xe0e4e09e,
    'AmbleWillBeRemoved,ThePrePartThatIs': 1656271935, //0x62b8b43f,
    '': 275646681, //0x106e08d9,
    // KAFKAJS, CLIENT FOR NODE.JS
    // https://github.com/tulios/kafkajs/blob/master/src/producer/partitioners/default/murmur2.spec.js
    '0': 971027396,
    '1': -1993445489,
    '128': -326012175,
    '2187': -1508407203,
    '16384': -325739742,
    '78125': -1654490814,
    '279936': 1462227128,
    '823543': -2014198330,
    '2097152': 607668903,
    '4782969': -1182699775,
    '10000000': -1830336757,
    '19487171': -1603849305,
    '35831808': -857013643,
    '62748517': -1167431028,
    '105413504': -381294639,
    '170859375': -1658323481,
    '100:48069': 1009543857,
  };

  test('murmur2', () {
    for (var e in testData.entries) {
      var data = e.key.codeUnits;
      int hash = Murmur2.eval(data);
      expect(hash, equals(e.value), reason: 'for key "${e.key}" expect ${e.value}');
    }
  });
}