part of kafka;

/// Initial contact point with a Kafka cluster.
class ContactPoint {
  final String host;
  final int port;

  ContactPoint(this.host, this.port);
}

/// Session responsible for communication with Kafka cluster.
///
/// In order to create new Session you need to pass a list of [ContactPoint]s to
/// the constructor. Each ContactPoint is defined by a host and a port of one
/// of the Kafka brokers. At least one ContactPoint is required to connect to
/// the cluster, all the rest members of the cluster will be automatically
/// detected by the Session.
///
/// For production deployments it is recommended to provide more than one
/// ContactPoint since this will enable "failover" in case one of the instances
/// is temporarily unavailable.
class KafkaSession {
  /// List of Kafka brokers which are used as initial contact points.
  final Queue<ContactPoint> contactPoints;

  Map<String, Future<Socket>> _sockets = Map();
  Map<String, StreamSubscription> _subscriptions = Map();
  Map<String, List<int>> _buffers = Map();
  Map<String, int> _sizes = Map();
  Map<KafkaRequest, Completer> _inflightRequests = Map();
  Map<Socket, Future> _flushFutures = Map();

  // Cluster Metadata
  List<Broker>? _brokers;
  Map<String, TopicMetadata> _topicsMetadata = Map();
  Map<String, int> topicCounter = Map();

  /// Creates new session.
  ///
  /// [contactPoints] will be used to fetch Kafka metadata information. At least
  /// one is required. However for production consider having more than 1.
  /// In case of one of the hosts is temporarily unavailable the session will
  /// rotate them until sucessful response is returned. Error will be thrown
  /// when all of the default hosts are unavailable.
  KafkaSession(List<ContactPoint> contactPoints) : contactPoints = Queue.from(contactPoints);

  /// Returns names of all existing topics in the Kafka cluster.
  Future<Set<String>> listTopics() async {
    // TODO: actually rotate default hosts on failure.
    var contactPoint = _getCurrentContactPoint();
    var request = new MetadataRequest();
    MetadataResponse response = await _send(contactPoint.host, contactPoint.port, request);

    return response.topics.map((_) => _.topicName).toSet();
  }

  /// Fetches Kafka cluster metadata. If [topicNames] is null then metadata for
  /// all topics will be returned.
  ///
  /// Please note that requests to fetch __all__ topics can not be cached by
  /// the client, so it may not be as performant as requesting topics
  /// explicitely.
  ///
  /// Also, if Kafka server is configured to auto-create topics you must
  /// explicitely specify topic name in metadata request, otherwise topic
  /// will not be created.
  Future<ClusterMetadata> getMetadata(Set<String> topicNames, {bool invalidateCache = false}) async {
    if (topicNames.isEmpty) throw new ArgumentError.value(topicNames, 'topicNames', 'List of topic names can not be empty');

    if (invalidateCache) {
      _brokers = null;
      _topicsMetadata = new Map();
    }
    // TODO: actually rotate default hosts on failure.
    var contactPoint = _getCurrentContactPoint();

    var topicsToFetch = topicNames.where((t) => !_topicsMetadata.keys.contains(t));
    if (topicsToFetch.length > 0) {
      MetadataResponse response = await _sendMetadataRequest(topicsToFetch.toSet(), contactPoint.host, contactPoint.port);
      response.topics.forEach((topic) {
        if (topicNames.contains(topic.topicName)) {
          _topicsMetadata[topic.topicName] = topic;
        }
      });
      _brokers = response.brokers;
    }
    List<TopicMetadata> metadata = List.unmodifiable([for(var name in topicNames) _topicsMetadata[name] ?? null].whereType<TopicMetadata>());

    return ClusterMetadata(_brokers ?? [], metadata);
  }

  Future<MetadataResponse> _sendMetadataRequest(Set<String> topics, String host, int port) async {
    var request = MetadataRequest(topics);
    MetadataResponse response = await _send(host, port, request);

    TopicMetadata? topicWithError = response.topics.firstWhereOrNull((_) => _.errorCode != KafkaServerError.NoError);

    if (topicWithError is TopicMetadata) {
      var retries = 1;
      var error = KafkaServerError(topicWithError.errorCode);
      while (error.isLeaderNotAvailable && retries < 5) {
        var future = Future.delayed(Duration(seconds: retries), () => _send(host, port, request));

        response = await future;
        topicWithError = response.topics.firstWhereOrNull((_) => _.errorCode != KafkaServerError.NoError);
        var errorCode = (topicWithError is TopicMetadata) ? topicWithError.errorCode : 0;
        error = KafkaServerError(errorCode);
        retries++;
      }

      if (error.isError) throw error;
    }

    return response;
  }

  /// Fetches metadata for specified [consumerGroup].
  ///
  /// It handles `ConsumerCoordinatorNotAvailableCode(15)` API error which Kafka
  /// returns in case [GroupCoordinatorRequest] is sent for the very first time
  /// to this particular broker (when special topic to store consumer offsets
  /// does not exist yet).
  ///
  /// It will attempt up to 5 retries (with linear delay) in order to fetch
  /// metadata.
  Future<GroupCoordinatorResponse> getConsumerMetadata(String consumerGroup) async {
    // TODO: rotate default hosts.
    var contactPoint = _getCurrentContactPoint();
    var request = new GroupCoordinatorRequest(consumerGroup);

    GroupCoordinatorResponse response = await _send(contactPoint.host, contactPoint.port, request);
    var retries = 1;
    var error = new KafkaServerError(response.errorCode);
    while (error.isConsumerCoordinatorNotAvailable && retries < 5) {
      var future = new Future.delayed(new Duration(seconds: retries), () => _send(contactPoint.host, contactPoint.port, request));

      response = await future;
      error = new KafkaServerError(response.errorCode);
      retries++;
    }

    if (error.isError) throw error;

    return response;
  }

  /// Sends request to specified [Broker].
  Future<dynamic> send(Broker broker, KafkaRequest request) {
    return _send(broker.host, broker.port, request);
  }

  Future<dynamic> _send(String host, int port, KafkaRequest request) async {
    kafkaLogger.finer('Session: Sending request ${request} to ${host}:${port}');
    var socket = await _getSocket(host, port);
    Completer completer = new Completer();
    _inflightRequests[request] = completer;

    /// Writing to socket is synchronous, so we need to remember future
    /// returned by last call to `flush` and only write this request after
    /// previous one has been flushed.
    var flushFuture = _flushFutures[socket];
    _flushFutures[socket] = flushFuture!.then((_) {
      socket.add(request.toBytes());
      return socket.flush().catchError((error) {
        _inflightRequests.remove(request);
        completer.completeError(error);
        return new Future.value();
      });
    });

    completer.future.timeout(Duration(seconds: 30)).catchError((error) {
      if (!completer.isCompleted) {
        completer.completeError(error);
        _inflightRequests.remove(request);
      }
    }, test: (e) => e is TimeoutException);

    completer.future.catchError((error) {
      socket.close();
    });

    return completer.future;
  }

  /// Closes this session and terminates all open socket connections.
  ///
  /// After session has been closed it can't be used or re-opened.
  Future close() async {
    for (var h in _sockets.keys) {
      await _subscriptions[h]?.cancel();
      (await _sockets[h])?.destroy();
    }
    _sockets.clear();
  }

  void _handleData(String hostPort, List<int> d) {
    var buffer = _buffers[hostPort]!;

    buffer.addAll(d);
    if (buffer.length >= 4 && _sizes[hostPort] == -1) {
      var sizeBytes = buffer.sublist(0, 4);
      var reader = new KafkaBytesReader.fromBytes(sizeBytes);
      _sizes[hostPort] = reader.readInt32();
    }

    List<int>? extra;
    if (buffer.length > _sizes[hostPort]! + 4) {
      extra = buffer.sublist(_sizes[hostPort]! + 4);
      buffer.removeRange(_sizes[hostPort]! + 4, buffer.length);
    }

    if (buffer.length == _sizes[hostPort]! + 4) {
      var header = buffer.sublist(4, 8);
      var reader = KafkaBytesReader.fromBytes(header);
      var correlationId = reader.readInt32();
      var request = _inflightRequests.keys.firstWhere((r) => r.correlationId == correlationId);
      var completer = _inflightRequests[request];
      var response = request.createResponse(buffer);
      _inflightRequests.remove(request);
      buffer.clear();
      _sizes[hostPort] = -1;

      completer?.complete(response);
      if (extra != null && extra.isNotEmpty) {
        _handleData(hostPort, extra);
      }
    }
  }

  ContactPoint _getCurrentContactPoint() {
    return contactPoints.first;
  }

  // void _rotateDefaultHosts() {
  //   var current = defaultHosts.removeFirst();
  //   defaultHosts.addLast(current);
  // }

  Future<Socket> _getSocket(String host, int port) {
    var key = '${host}:${port}';
    if (!_sockets.containsKey(key)) {
      _sockets[key] = Socket.connect(host, port);
      _sockets[key]?.then((socket) {
        socket.setOption(SocketOption.tcpNoDelay, true);
        _buffers[key] = [];
        _sizes[key] = -1;
        _subscriptions[key] = socket.listen((d) => _handleData(key, d));
        _flushFutures[socket] = new Future.value();
        socket.done.whenComplete(() {
          _sockets.remove(key);
          socket.destroy();
          _subscriptions.remove(key)?.cancel();
          _flushFutures.remove(socket);
        });
      }, onError: (error) {
        _sockets.remove(key);
      });
    }

    return _sockets[key]!;
  }
}

/// Stores metadata information about cluster including available brokers
/// and topics.
class ClusterMetadata {
  /// List of brokers in the cluster.
  final List<Broker> brokers;

  /// List with metadata for each topic.
  final List<TopicMetadata> topics;

  /// Creates new instance of cluster metadata.
  ClusterMetadata(this.brokers, this.topics);

  /// Returns [Broker] by specified [nodeId].
  Broker getBroker(int nodeId) {
    return brokers.firstWhere((b) => b.id == nodeId);
  }

  /// Returns [TopicMetadata] for specified [topicName].
  ///
  /// If no topic is found will throw `StateError`.
  TopicMetadata getTopicMetadata(String topicName) {
    return topics.firstWhere((topic) => topic.topicName == topicName, orElse: () => throw new StateError('No topic ${topicName} found in metadata.'));
  }
}

extension FirstWhereOrNullExtension<E> on Iterable<E> {
  E? firstWhereOrNull(bool Function(E) test) {
    for (E element in this) {
      if (test(element)) return element;
    }
    return null;
  }
}
