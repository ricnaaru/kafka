import 'dart:async';

typedef TestFunction = bool Function(dynamic error);

Future<T> retryAsync<T>(
  Future<T> func(),
  int retries,
  Duration delay, {
  TestFunction? test,
}) {
  return func().catchError((error) {
    if (retries == 0) {
      return Future.value(error);
    } else {
      if (test != null && !test(error)) {
        return Future.value(error);
      }
      return new Future.delayed(delay, () => retryAsync(func, retries - 1, delay));
    }
  });
}
