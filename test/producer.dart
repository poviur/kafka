import 'package:test/test.dart';
import 'package:kafkabr/kafka.dart';

void main() {
  test('Simple message producing', () async {
    var host = ContactPoint('127.0.0.1', 9092);
    var session = new KafkaSession([host]);

    var producer = new Producer(session, 1, 1000);
    var result = await producer.produce([
      new ProduceEnvelope('quickstart-events', 0, [new Message('TestMessage 1'.codeUnits, key: 'test'.codeUnits)]),
      new ProduceEnvelope('quickstart-events', 0, [new Message('TestMessage 2'.codeUnits)]),
    ]);
    print(result.hasErrors);
    print(result.offsets);
    session.close();
  });

  test('RoundRobbin partitioning', () async {
    var host = ContactPoint('127.0.0.1', 9092);
    var session = new KafkaSession([host]);

    var producer = new Producer(session, 1, 1000);
    var result = await producer.produce([
      ProduceEnvelope('MobileTest', null, [Message('TestMessage 1'.codeUnits)]),
      ProduceEnvelope('MobileTest', null, [Message('TestMessage 2'.codeUnits)]),
      ProduceEnvelope('MobileTest', null, [Message('TestMessage 3'.codeUnits)]),
    ]);
    print(result.hasErrors);
    print(result.offsets);
    session.close();
  });

  test('Key partitioning', () async {
    var host = ContactPoint('127.0.0.1', 9092);
    var session = new KafkaSession([host]);

    var producer = new Producer(session, 1, 1000);
    var result = await producer.produce([
      ProduceEnvelope('MobileTest', null, [Message('TestMessage 1'.codeUnits, key: 'TestMessageKey'.codeUnits)]),
      ProduceEnvelope('MobileTest', null, [Message('TestMessage 2'.codeUnits, key: 'TestMessageKey'.codeUnits)]),
      ProduceEnvelope('MobileTest', null, [Message('TestMessage 3'.codeUnits, key: 'YetAnotherMessageKey'.codeUnits)]),
      ProduceEnvelope('MobileTest', 2, [Message('TestMessage 4'.codeUnits, key: 'AnotherMessageKey'.codeUnits)], compression: KafkaCompression.gzip),
    ]);
    print(result.hasErrors);
    print(result.offsets);
    session.close();
  });
}