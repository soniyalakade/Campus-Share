import { randomUUID } from 'node:crypto';
import { createId } from './store.js';

const terminalRelayStatuses = ['relayed', 'accepted_no_clients'];

const numericSetting = (name, fallback, minimum = 1) => {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed >= minimum ? parsed : fallback;
};

export function createOutboxEntry(department, eventType, payload, eventId = createId('event')) {
  return {
    eventId,
    eventType,
    sourceNode: department,
    sourceDepartment: department,
    timestamp: new Date(),
    resourceId: payload.resourceId,
    reservationId: payload.reservationId || null,
    borrowRecordId: payload.borrowRecordId || null,
    payload,
    rabbitmqStatus: 'pending',
    websocketRelayStatus: 'pending',
    attempts: 0,
    nextAttemptAt: new Date(),
    claimedBy: null,
    leaseUntil: null,
    completedAt: null,
    lastErrorCode: null
  };
}

export function createOutboxPublisher({
  Resource,
  department,
  publishRabbit,
  relayEvent,
  pollIntervalMs = numericSetting('OUTBOX_POLL_INTERVAL_MS', 1000),
  leaseMs = numericSetting('OUTBOX_LEASE_MS', 30000),
  retryBaseMs = numericSetting('OUTBOX_RETRY_BASE_MS', 1000),
  retryMaxMs = numericSetting('OUTBOX_RETRY_MAX_MS', 60000),
  completedRetentionMs = numericSetting('OUTBOX_COMPLETED_RETENTION_MS', 7 * 24 * 60 * 60 * 1000),
  batchSize = numericSetting('OUTBOX_BATCH_SIZE', 10),
  log = console
}) {
  const ownerId = randomUUID();
  const activeEvents = new Map();
  const inFlightOperations = new Set();
  let timer = null;
  let stopping = true;
  let activePump = null;
  let rabbitConnection = null;
  let rabbitChannel = null;
  let rabbitConnectionPromise = null;
  let pendingCount = null;
  let successfulPublications = 0;
  let retryCount = 0;
  let persistentFailureCount = 0;

  const createRabbitChannel = async () => {
    if (rabbitChannel) return rabbitChannel;
    if (rabbitConnectionPromise) return rabbitConnectionPromise;
    rabbitConnectionPromise = (async () => {
      const amqp = await import('amqplib');
      const connection = await amqp.default.connect(
        process.env.RABBITMQ_URL || '******localhost:5672',
        { timeout: numericSetting('OUTBOX_PUBLISH_TIMEOUT_MS', 1000) }
      );
      const channel = await connection.createConfirmChannel();
      await channel.assertExchange('campus.resource.events', 'topic', { durable: true });
      connection.on('close', () => {
        if (rabbitConnection === connection) {
          rabbitConnection = null;
          rabbitChannel = null;
        }
      });
      connection.on('error', (error) => {
        log.warn(`${department} outbox RabbitMQ connection error (${error.code || error.name})`);
      });
      channel.on('error', (error) => {
        log.warn(`${department} outbox RabbitMQ channel error (${error.code || error.name})`);
        if (rabbitChannel === channel) rabbitChannel = null;
      });
      rabbitConnection = connection;
      rabbitChannel = channel;
      return channel;
    })();
    try {
      return await rabbitConnectionPromise;
    } finally {
      rabbitConnectionPromise = null;
    }
  };

  const closeRabbit = async () => {
    const connection = rabbitConnection;
    rabbitConnection = null;
    rabbitChannel = null;
    if (!connection) return;
    try {
      await connection.close();
    } catch (error) {
      log.warn(`${department} outbox RabbitMQ close failed (${error.code || error.name})`);
    }
  };

  const waitForDrain = async (channel) => {
    const timeoutMs = numericSetting('OUTBOX_PUBLISH_TIMEOUT_MS', 1000);
    let timerId;
    let onDrain;
    let onError;
    try {
      await Promise.race([
        new Promise((resolve, reject) => {
          onDrain = resolve;
          onError = reject;
          channel.once('drain', onDrain);
          channel.once('error', onError);
        }),
        new Promise((_, reject) => {
          timerId = setTimeout(() => reject(Object.assign(new Error('RabbitMQ channel drain timed out'), { code: 'DRAIN_TIMEOUT' })), timeoutMs);
        })
      ]);
    } finally {
      clearTimeout(timerId);
      if (onDrain) channel.off('drain', onDrain);
      if (onError) channel.off('error', onError);
    }
  };

  const sendRabbit = publishRabbit || (async (event) => {
    const channel = await createRabbitChannel();
    const accepted = channel.publish(
      'campus.resource.events',
      `campus.${department}.${event.eventType}`,
      Buffer.from(JSON.stringify(event)),
      { persistent: true, contentType: 'application/json', messageId: event.eventId, timestamp: Date.now() }
    );
    if (!accepted) await waitForDrain(channel);
    const timeoutMs = numericSetting('OUTBOX_CONFIRM_TIMEOUT_MS', 5000);
    let timer;
    try {
      await Promise.race([
        channel.waitForConfirms(),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(Object.assign(new Error('Publisher confirmation timed out'), { code: 'CONFIRM_TIMEOUT' })), timeoutMs);
        })
      ]);
    } finally {
      clearTimeout(timer);
    }
  });

  const sendRelay = relayEvent || (async (event) => {
    const gatewayPort = Number(process.env.GATEWAY_PORT || 4000);
    const gatewayUrl = process.env.GATEWAY_INTERNAL_URL || `http://localhost:${gatewayPort}`;
    const response = await fetch(`${gatewayUrl}/internal/events/notify`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Service-Token': process.env.INTERNAL_SERVICE_TOKEN || 'campus-share-local-internal-token'
      },
      body: JSON.stringify(event),
      signal: AbortSignal.timeout(numericSetting('OUTBOX_RELAY_TIMEOUT_MS', 1500))
    });
    if (!response.ok) {
      throw Object.assign(new Error('Gateway relay request failed'), { code: `HTTP_${response.status}` });
    }
    const result = await response.json();
    return result.websocketRelayStatus || (result.relayed ? 'relayed' : 'accepted_no_clients');
  });

  const eventFromEntry = (entry) => ({
    eventId: entry.eventId,
    eventType: entry.eventType,
    sourceNode: entry.sourceNode,
    sourceDepartment: entry.sourceDepartment,
    timestamp: new Date(entry.timestamp).toISOString(),
    ...(Number.isSafeInteger(entry.lamportTimestamp) ? { lamportTimestamp: entry.lamportTimestamp } : {}),
    resourceId: entry.resourceId,
    ...(entry.reservationId ? { reservationId: entry.reservationId } : {}),
    ...(entry.borrowRecordId ? { borrowRecordId: entry.borrowRecordId } : {}),
    payload: entry.payload || {},
    messagingStatus: {
      rabbitmq: entry.rabbitmqStatus === 'confirmed' ? 'published' : 'pending',
      websocketRelay: entry.websocketRelayStatus === 'pending' ? 'pending' : entry.websocketRelayStatus
    }
  });

  const eligibleEntry = (now, eventId) => ({
    ...(eventId ? { eventId } : {}),
    nextAttemptAt: { $lte: now },
    $and: [
      { $or: [{ rabbitmqStatus: 'pending' }, { websocketRelayStatus: 'pending' }] },
      { $or: [{ leaseUntil: null }, { leaseUntil: { $exists: false } }, { leaseUntil: { $lte: now } }] }
    ]
  });

  const updateClaimedEntry = (resourceId, eventId, update) => Resource.updateOne({
    resourceId,
    department,
    outbox: { $elemMatch: { eventId, claimedBy: ownerId } }
  }, update);

  const getDeliveryStatus = async (eventId) => {
    const resource = await Resource.findOne({ department, 'outbox.eventId': eventId })
      .select({ resourceId: 1, outbox: { $elemMatch: { eventId } } })
      .lean();
    const entry = resource?.outbox?.[0];
    if (!entry) return null;
    return {
      eventId,
      rabbitmq: entry.rabbitmqStatus === 'confirmed' ? 'published' : 'pending',
      websocketRelay: entry.websocketRelayStatus
    };
  };

  const recordPendingCount = async () => {
    const [result] = await Resource.aggregate([
      { $match: { department } },
      { $unwind: '$outbox' },
      { $match: { $or: [{ 'outbox.rabbitmqStatus': 'pending' }, { 'outbox.websocketRelayStatus': 'pending' }] } },
      { $count: 'pending' }
    ]);
    const nextCount = result?.pending || 0;
    if (nextCount !== pendingCount) {
      pendingCount = nextCount;
      log.info(`${department} outbox pending events: ${pendingCount}`);
    }
    return nextCount;
  };

  const cleanupCompleted = async () => {
    const completedBefore = new Date(Date.now() - completedRetentionMs);
    await Resource.updateMany({ department }, {
      $pull: {
        outbox: {
          rabbitmqStatus: 'confirmed',
          websocketRelayStatus: { $in: terminalRelayStatuses },
          completedAt: { $lte: completedBefore }
        }
      }
    });
    await Resource.deleteMany({ department, isDeleted: true, outbox: { $size: 0 } });
  };

  const processOne = async (eventId) => {
    if (eventId && activeEvents.has(eventId)) return activeEvents.get(eventId);
    const operation = (async () => {
      const now = new Date();
      const resource = await Resource.findOneAndUpdate({
        department,
        outbox: { $elemMatch: eligibleEntry(now, eventId) }
      }, {
        $set: {
          'outbox.$.claimedBy': ownerId,
          'outbox.$.leaseUntil': new Date(now.getTime() + leaseMs)
        },
        $inc: { 'outbox.$.attempts': 1 }
      }, { new: true }).lean();
      if (!resource) return null;
      const entry = resource.outbox.find((item) => item.eventId === eventId && item.claimedBy === ownerId)
        || resource.outbox.find((item) => item.claimedBy === ownerId);
      if (!entry) return null;
      const event = eventFromEntry(entry);
      const updates = {};
      let failureCode = null;
      if (entry.rabbitmqStatus !== 'confirmed') {
        try {
          await sendRabbit(event);
          updates['outbox.$.rabbitmqStatus'] = 'confirmed';
          event.messagingStatus.rabbitmq = 'published';
          successfulPublications += 1;
          log.info(`${department} outbox RabbitMQ confirmation received for ${entry.eventId}`);
        } catch (error) {
          failureCode = String(error.code || error.name || 'PUBLISH_FAILED').slice(0, 64);
          log.warn(`${department} outbox RabbitMQ retry ${entry.attempts} for ${entry.eventId} (${failureCode})`);
          if (!publishRabbit) await closeRabbit();
        }
      }
      if (entry.websocketRelayStatus === 'pending') {
        try {
          const status = await sendRelay(event);
          if (!terminalRelayStatuses.includes(status)) {
            throw Object.assign(new Error('Gateway did not accept the event for relay'), { code: 'INVALID_RELAY_STATUS' });
          }
          updates['outbox.$.websocketRelayStatus'] = status;
        } catch (error) {
          failureCode ||= String(error.code || error.name || 'RELAY_FAILED').slice(0, 64);
          log.warn(`${department} outbox Gateway relay retry ${entry.attempts} for ${entry.eventId} (${failureCode})`);
        }
      }
      const completed = (updates['outbox.$.rabbitmqStatus'] === 'confirmed' || entry.rabbitmqStatus === 'confirmed')
        && (updates['outbox.$.websocketRelayStatus'] || entry.websocketRelayStatus) !== 'pending';
      const update = { $set: updates, $unset: { 'outbox.$.claimedBy': 1, 'outbox.$.leaseUntil': 1 } };
      if (completed) {
        update.$set['outbox.$.completedAt'] = new Date();
      } else {
        const exponent = Math.min(Math.max(entry.attempts - 1, 0), 30);
        const delay = Math.min(retryBaseMs * (2 ** exponent), retryMaxMs);
        update.$set['outbox.$.nextAttemptAt'] = new Date(Date.now() + delay);
        update.$set['outbox.$.lastErrorCode'] = failureCode || 'DELIVERY_PENDING';
        retryCount += 1;
        if (entry.attempts >= 10) {
          persistentFailureCount += 1;
          log.error(`${department} outbox event ${entry.eventId} remains pending after ${entry.attempts} attempts`);
        }
      }
      const result = await updateClaimedEntry(resource.resourceId, entry.eventId, update);
      if (!result.matchedCount) {
        log.warn(`${department} outbox claim expired before update for ${entry.eventId}`);
      }
      await recordPendingCount();
      if (resource.isDeleted) await cleanupCompleted();
      return getDeliveryStatus(entry.eventId);
    })();
    if (eventId) activeEvents.set(eventId, operation);
    inFlightOperations.add(operation);
    try {
      return await operation;
    } finally {
      inFlightOperations.delete(operation);
      if (eventId) activeEvents.delete(eventId);
    }
  };

  const pump = async () => {
    if (stopping || activePump) return activePump;
    activePump = (async () => {
      try {
        await cleanupCompleted();
        for (let index = 0; index < batchSize && !stopping; index += 1) {
          const processed = await processOne();
          if (!processed) break;
        }
        await recordPendingCount();
      } catch (error) {
        log.error(`${department} outbox worker database operation failed (${error.code || error.name})`);
      } finally {
        activePump = null;
        if (!stopping) timer = setTimeout(pump, pollIntervalMs);
      }
    })();
    return activePump;
  };

  const start = async () => {
    if (!stopping) return;
    stopping = false;
    await recordPendingCount();
    timer = setTimeout(pump, 0);
  };

  const stop = async (timeoutMs = 10000) => {
    stopping = true;
    if (timer) clearTimeout(timer);
    timer = null;
    const running = [...inFlightOperations, ...(activePump ? [activePump] : [])];
    if (running.length) {
      let timerId;
      await Promise.race([
        Promise.allSettled(running),
        new Promise((resolve) => { timerId = setTimeout(resolve, timeoutMs); })
      ]);
      clearTimeout(timerId);
    }
    await closeRabbit();
  };

  const processEvent = async (eventId) => {
    await processOne(eventId);
    return getDeliveryStatus(eventId);
  };

  return {
    start,
    stop,
    processEvent,
    processOne,
    cleanupCompleted,
    getMetrics: () => ({ pendingEvents: pendingCount, successfulPublications, retryCount, persistentFailureCount })
  };
}
