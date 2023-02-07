import 'dart:async';

import 'package:kafka/kafka.dart';
import 'package:logging/logging.dart';

Future main() async {
  Logger.root.level = Level.ALL;
  Logger.root.onRecord.listen(print);

  var session = Session(['15.235.180.29:9093']);
  var consumer = Consumer<String, String>('simple_consumer', StringDeserializer(), StringDeserializer(), session);

  consumer.subscribe(['simple_topic']);
  var queue = consumer.poll();
  while (await queue.moveNext()) {
    var records = queue.current;
    if (records != null) {
      for (var record in records.records) {
        print(
            "[${record.topic}:${record.partition}], offset: ${record.offset}, ${record.key}, ${record
                .value}, ts: ${record.timestamp}");
      }
      await consumer.commit();
    }
  }
  await session.close();
}
