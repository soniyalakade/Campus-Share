import express from 'express';
import cors from 'cors';
import http from 'node:http';
import * as grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import dotenv from 'dotenv';
import { createId, createResource, seedResources, nowISO } from '../shared/store.js';
import { connectDatabase, disconnectDatabase, isDatabaseReady } from '../shared/database.js';
import { createOutboxEntry, createOutboxPublisher } from '../shared/outboxPublisher.js';
import { LamportClock } from '../shared/lamportClock.js';
import { BullyElection } from '../shared/bullyElection.js';
import { DEPARTMENTS } from '../shared/constants.js';
import Resource from './models/Resource.js';

const rootEnvPath = path.resolve(process.cwd(), '.env');
dotenv.config({ path: rootEnvPath });

const dateToISO = (value) => (value instanceof Date ? value.toISOString() : value);

export function createDepartmentService({ department, httpPort, grpcPort, gatewayPort = Number(process.env.GATEWAY_PORT || 4000) }) {
  const app = express();
  const mongoUri = process.env[`${department}_MONGO_URI`]
    || `mongodb://localhost:27017/${department.toLowerCase()}_db`;
  const internalServiceToken = process.env.INTERNAL_SERVICE_TOKEN || 'campus-share-local-internal-token';
  const configuredOutboxLimit = Number(process.env.OUTBOX_MAX_ENTRIES_PER_RESOURCE);
  const maxOutboxEntries = Number.isSafeInteger(configuredOutboxLimit) && configuredOutboxLimit > 0
    ? configuredOutboxLimit
    : 500;
  const lamportClock = new LamportClock();
  const defaultPorts = { CS: 5001, IT: 5002, ENTC: 5003 };
  const peerServiceUrls = Object.fromEntries(DEPARTMENTS.map((node) => [
    node,
    process.env[`${node}_SERVICE_URL`] || `http://localhost:${defaultPorts[node]}`
  ]));
  const electionTimeoutMs = Number(process.env.ELECTION_TIMEOUT_MS) || 1200;
  const peerTimeoutMs = Number(process.env.PEER_HEALTH_TIMEOUT_MS) || 1200;
  const callPeer = async (node, pathName, body, timeoutMs = electionTimeoutMs) => {
    const response = await fetch(`${peerServiceUrls[node]}${pathName}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Service-Token': internalServiceToken
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs)
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw Object.assign(new Error(payload.error || `Peer request failed (${response.status})`), { status: response.status });
    }
    return payload;
  };
  const checkPeer = async (node) => {
    const response = await fetch(`${peerServiceUrls[node]}/internal/health`, {
      signal: AbortSignal.timeout(peerTimeoutMs)
    });
    if (!response.ok) return false;
    const health = await response.json().catch(() => ({}));
    return health.status === 'healthy';
  };
  const bullyElection = new BullyElection({
    department,
    clock: lamportClock,
    sendMessage: callPeer,
    checkPeer,
    timeoutMs: electionTimeoutMs,
    coordinatorWaitMs: Number(process.env.ELECTION_COORDINATOR_WAIT_MS) || 3000,
    retryDelayMs: Number(process.env.ELECTION_RETRY_DELAY_MS) || 200
  });
  let outboxPublisher;
  const vectorClock = { CS: 0, IT: 0, ENTC: 0 };
  let lastHeartbeat = nowISO();
  let nodeHealth = 'healthy';
  let electionMonitor = null;
  let electionStartupTimer = null;
  let heartbeatTimer = null;

  const toResourcePayload = (item) => ({
    resourceId: item.resourceId,
    name: item.name,
    category: item.category,
    description: item.description,
    department: item.department,
    laboratory: item.laboratory,
    totalQuantity: item.totalQuantity,
    availableQuantity: item.availableQuantity,
    reservedQuantity: item.reservedQuantity,
    borrowedQuantity: item.borrowedQuantity,
    condition: item.condition,
    imageUrl: item.imageUrl,
    manualUrl: item.manualUrl,
    tutorialVideoUrl: item.tutorialVideoUrl,
    createdAt: dateToISO(item.createdAt),
    updatedAt: dateToISO(item.updatedAt),
    version: item.version,
    vectorClock: { ...item.vectorClock }
  });

  const makeOutboxEvent = (eventType, payload) => ({
    ...createOutboxEntry(department, eventType, payload),
    lamportTimestamp: lamportClock.tick()
  });
  const appendOutboxEvent = (update, event) => ({
    ...update,
    $push: { ...update.$push, outbox: event }
  });
  const withOutboxCapacity = (query) => {
    const { $expr, ...rest } = query;
    return {
      ...rest,
      $and: [
        ...(rest.$and || []),
        ...($expr ? [{ $expr }] : []),
        { $expr: { $lt: [{ $size: { $ifNull: ['$outbox', []] } }, maxOutboxEntries] } }
      ]
    };
  };
  const isOutboxAtCapacity = async (resourceId) => {
    const resource = await Resource.findOne({ department, resourceId, isDeleted: { $ne: true } })
      .select('outbox')
      .lean();
    return Boolean(resource && resource.outbox.length >= maxOutboxEntries);
  };
  const tryDeliverOutboxEvent = async (eventId) => (
    await outboxPublisher.processEvent(eventId)
  ) || { eventId, rabbitmq: 'pending', websocketRelay: 'pending' };

  const requireInternalService = (req, res, next) => {
    if (req.get('X-Internal-Service-Token') !== internalServiceToken) {
      return res.status(401).json({ error: 'Invalid internal service credentials' });
    }
    return next();
  };

  const actorFromRequest = (req) => ({
    userId: req.get('X-Actor-Id'),
    role: req.get('X-Actor-Role'),
    department: req.get('X-Actor-Department') || null
  });

  const authorizeLifecycleActor = (req, res, allowedRoles) => {
    const actor = actorFromRequest(req);
    if (!actor.userId || !allowedRoles.includes(actor.role)) {
      res.status(403).json({ error: 'This operation is not permitted for this user' });
      return null;
    }
    if (actor.role === 'DEPARTMENT_ADMIN' && actor.department !== department) {
      res.status(403).json({ error: 'Department ownership mismatch' });
      return null;
    }
    return actor;
  };

  const hasValidQuantities = (resource) => (
    Number.isInteger(resource.totalQuantity)
    && Number.isInteger(resource.availableQuantity)
    && Number.isInteger(resource.reservedQuantity)
    && Number.isInteger(resource.borrowedQuantity)
    && resource.totalQuantity >= 0
    && resource.availableQuantity >= 0
    && resource.reservedQuantity >= 0
    && resource.borrowedQuantity >= 0
    && resource.totalQuantity === resource.availableQuantity + resource.reservedQuantity + resource.borrowedQuantity
  );

  const withResourceMetadata = (update = {}, event) => appendOutboxEvent({
    ...update,
    $set: { ...update.$set, updatedAt: new Date() },
    $inc: {
      ...update.$inc,
      version: 1,
      [`vectorClock.${department}`]: 1
    }
  }, event);

  const resourceMutation = (fields = {}, event) => withResourceMetadata({ $set: fields }, event);

  const observeResourceClock = (resource) => {
    vectorClock[department] = Number(resource.vectorClock?.[department] || 0);
  };

  app.use(cors());
  app.use(express.json());

  app.get('/internal/health', (req, res) => {
    if (!isDatabaseReady()) {
      return res.status(503).json({
        department,
        nodeId: department,
        status: 'database_unavailable',
        timestamp: nowISO()
      });
    }
    res.json({
      department,
      nodeId: department,
      status: nodeHealth,
      lastHeartbeat,
      leader: bullyElection.getState().coordinator,
      electionStatus: bullyElection.getState().electionStatus,
      priority: bullyElection.getState().priority,
      lamportClock: lamportClock.getTimestamp(),
      timestamp: nowISO()
    });
  });

  app.get('/internal/distributed/lamport', (req, res) => {
    res.json({ department, lamportClock: lamportClock.getTimestamp(), timestamp: nowISO() });
  });

  app.get('/internal/distributed/vector', (req, res) => {
    res.json({ department, vectorClock, timestamp: nowISO() });
  });

  app.get('/internal/leader', requireInternalService, (_req, res) => {
    res.json({ ...bullyElection.getState(), timestamp: nowISO() });
  });

  app.post('/internal/leader/announce', requireInternalService, async (req, res) => {
    try {
      const body = req.body || {};
      const sender = body.sender || body.coordinator;
      return res.json(await bullyElection.receiveMessage({
        ...body,
        type: 'COORDINATOR',
        sender,
        messageId: body.messageId || `${body.electionId}:${sender}:coordinator`,
        electionId: body.electionId || `legacy-${sender}`
      }));
    } catch (error) {
      return res.status(400).json({ error: error.message });
    }
  });

  app.get('/internal/election', requireInternalService, (_req, res) => {
    res.json(bullyElection.getState());
  });

  app.post('/internal/election', requireInternalService, async (_req, res, next) => {
    try {
      return res.json(await bullyElection.startElection('explicit-trigger'));
    } catch (error) {
      return next(error);
    }
  });

  app.post('/internal/election/message', requireInternalService, async (req, res, next) => {
    try {
      return res.json(await bullyElection.receiveMessage(req.body));
    } catch (error) {
      return res.status(400).json({ error: error.message });
    }
  });

  app.get('/internal/resources', requireInternalService, async (_req, res, next) => {
    try {
      const items = await Resource.find({ department, isDeleted: { $ne: true } }).sort({ name: 1 }).lean();
      res.json(items.map(toResourcePayload));
    } catch (error) {
      next(error);
    }
  });

  app.get('/internal/resources/:id', requireInternalService, async (req, res, next) => {
    try {
      const resource = await Resource.findOne({ resourceId: req.params.id, department, isDeleted: { $ne: true } }).lean();
      if (!resource) return res.status(404).json({ error: 'Resource not found' });
      return res.json(toResourcePayload(resource));
    } catch (error) {
      return next(error);
    }
  });

  app.post('/internal/resources', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['DEPARTMENT_ADMIN']);
      if (!actor) return;
      const body = req.body || {};
      if (!body.name || !body.category) return res.status(400).json({ error: 'Name and category are required' });
      const resource = createResource({
        department,
        name: String(body.name).trim(),
        category: String(body.category).trim(),
        description: body.description || '',
        laboratory: body.laboratory || 'Main Lab',
        totalQuantity: Number(body.totalQuantity ?? body.availableQuantity ?? 1),
        availableQuantity: Number(body.availableQuantity ?? body.totalQuantity ?? 1),
        reservedQuantity: Number(body.reservedQuantity ?? 0),
        borrowedQuantity: Number(body.borrowedQuantity ?? 0),
        condition: body.condition || 'Good',
        imageUrl: body.imageUrl || '',
        manualUrl: body.manualUrl || '',
        tutorialVideoUrl: body.tutorialVideoUrl || ''
      });
      if (!hasValidQuantities(resource)) {
        return res.status(400).json({ error: 'Quantities must be non-negative and sum to totalQuantity' });
      }
      const event = makeOutboxEvent('RESOURCE_ADDED', { resourceId: resource.resourceId, department });
      const created = await Resource.create({ ...resource, outbox: [event] });
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.status(201).json({ ...toResourcePayload(created.toObject()), eventDelivery });
    } catch (error) {
      return next(error);
    }
  });

  app.put('/internal/resources/:id', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['DEPARTMENT_ADMIN']);
      if (!actor) return;
      const { id } = req.params;
      const current = await Resource.findOne({ resourceId: id, department, isDeleted: { $ne: true } }).lean();
      if (!current) return res.status(404).json({ error: 'Resource not found' });
      const body = req.body || {};
      const fields = {
        name: body.name ?? current.name,
        category: body.category ?? current.category,
        description: body.description ?? current.description,
        laboratory: body.laboratory ?? current.laboratory,
        totalQuantity: Number(body.totalQuantity ?? current.totalQuantity),
        availableQuantity: Number(body.availableQuantity ?? current.availableQuantity),
        reservedQuantity: current.reservedQuantity,
        borrowedQuantity: current.borrowedQuantity,
        condition: body.condition ?? current.condition,
        imageUrl: body.imageUrl ?? current.imageUrl,
        manualUrl: body.manualUrl ?? current.manualUrl,
        tutorialVideoUrl: body.tutorialVideoUrl ?? current.tutorialVideoUrl
      };
      if ((body.reservedQuantity !== undefined && Number(body.reservedQuantity) !== current.reservedQuantity)
        || (body.borrowedQuantity !== undefined && Number(body.borrowedQuantity) !== current.borrowedQuantity)) {
        return res.status(400).json({ error: 'Reserved and borrowed quantities are managed by the lifecycle' });
      }
      if (!hasValidQuantities(fields)) {
        return res.status(400).json({ error: 'Quantities must be non-negative and sum to totalQuantity' });
      }
      const event = makeOutboxEvent('RESOURCE_UPDATED', { resourceId: id, department });
      const update = resourceMutation(fields, event);
      const updated = await Resource.findOneAndUpdate(
        withOutboxCapacity({ resourceId: id, department, isDeleted: { $ne: true }, version: current.version }),
        update,
        { new: true, runValidators: true }
      ).lean();
      if (!updated) {
        if (await isOutboxAtCapacity(id)) return res.status(503).json({ error: 'Resource event outbox is full; retry after delivery recovery' });
        return res.status(409).json({ error: 'Resource was changed concurrently; reload and retry' });
      }
      observeResourceClock(updated);
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.json({ ...toResourcePayload(updated), eventDelivery });
    } catch (error) {
      return next(error);
    }
  });

  app.delete('/internal/resources/:id', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['DEPARTMENT_ADMIN']);
      if (!actor) return;
      const id = req.params.id;
      const event = makeOutboxEvent('RESOURCE_DELETED', { resourceId: id, department });
      const removed = await Resource.findOneAndUpdate(withOutboxCapacity({
        resourceId: id,
        department,
        isDeleted: { $ne: true },
        reservations: { $not: { $elemMatch: { status: { $in: ['PENDING', 'APPROVED'] } } } },
        borrowRecords: { $not: { $elemMatch: { status: 'BORROWED' } } }
      }), resourceMutation({ isDeleted: true }, event), { new: true }).lean();
      if (!removed) {
        if (await isOutboxAtCapacity(id)) return res.status(503).json({ error: 'Resource event outbox is full; retry after delivery recovery' });
        const exists = await Resource.exists({ resourceId: id, department, isDeleted: { $ne: true } });
        if (!exists) return res.status(404).json({ error: 'Resource not found' });
        return res.status(409).json({ error: 'Cannot delete a resource with active reservations or borrow records' });
      }
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.json({ deleted: removed.resourceId, eventDelivery });
    } catch (error) {
      return next(error);
    }
  });

  app.get('/internal/reservations', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['STUDENT', 'DEPARTMENT_ADMIN']);
      if (!actor) return;
      if (actor.role === 'STUDENT' && req.query.studentId !== actor.userId) {
        return res.status(403).json({ error: 'Students may only list their own reservations' });
      }
      const matching = actor.role === 'STUDENT' ? { 'reservations.studentId': actor.userId } : {};
      const resources = await Resource.find({ department, isDeleted: { $ne: true }, ...matching }).select('reservations').lean();
      const records = resources.flatMap((item) => item.reservations || [])
        .filter((item) => actor.role === 'DEPARTMENT_ADMIN' || item.studentId === actor.userId);
      return res.json(records);
    } catch (error) {
      return next(error);
    }
  });

  app.get('/internal/reservations/:id', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['STUDENT', 'DEPARTMENT_ADMIN']);
      if (!actor) return;
      const resource = await Resource.findOne({
        department,
        isDeleted: { $ne: true },
        'reservations.reservationId': req.params.id
      }).select('reservations').lean();
      const reservation = resource?.reservations?.find((item) => item.reservationId === req.params.id);
      if (!reservation) return res.status(404).json({ error: 'Reservation not found' });
      if (actor.role === 'STUDENT' && reservation.studentId !== actor.userId) {
        return res.status(403).json({ error: 'You may only view your own reservation' });
      }
      return res.json(reservation);
    } catch (error) {
      return next(error);
    }
  });

  app.post('/internal/resources/:id/reservations', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['STUDENT']);
      if (!actor) return;
      const reservation = {
        reservationId: createId('reservation'),
        resourceId: req.params.id,
        department,
        studentId: actor.userId,
        status: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date()
      };
      const event = makeOutboxEvent('RESOURCE_RESERVED', {
        resourceId: req.params.id,
        reservationId: reservation.reservationId,
        studentId: actor.userId,
        status: reservation.status
      });
      const updated = await Resource.findOneAndUpdate(withOutboxCapacity({
        resourceId: req.params.id,
        department,
        isDeleted: { $ne: true },
        availableQuantity: { $gte: 1 },
        $expr: { $eq: ['$totalQuantity', { $add: ['$availableQuantity', '$reservedQuantity', '$borrowedQuantity'] }] }
      }), withResourceMetadata({
        $inc: { availableQuantity: -1, reservedQuantity: 1 },
        $push: { reservations: reservation }
      }, event), { new: true, runValidators: true }).lean();
      if (!updated) {
        if (await isOutboxAtCapacity(req.params.id)) return res.status(503).json({ error: 'Resource event outbox is full; retry after delivery recovery' });
        const exists = await Resource.exists({ resourceId: req.params.id, department, isDeleted: { $ne: true } });
        if (!exists) return res.status(404).json({ error: 'Resource not found in this department' });
        return res.status(409).json({ error: 'No available quantity to reserve or resource quantity invariant failed' });
      }
      observeResourceClock(updated);
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.status(201).json({
        reservation,
        resource: toResourcePayload(updated),
        eventDelivery
      });
    } catch (error) {
      return next(error);
    }
  });

  app.patch('/internal/reservations/:id/status', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['DEPARTMENT_ADMIN']);
      if (!actor) return;
      const nextStatus = req.body?.status;
      if (!['APPROVED', 'REJECTED'].includes(nextStatus)) {
        return res.status(400).json({ error: 'status must be APPROVED or REJECTED' });
      }
      const currentResource = await Resource.findOne({
        department,
        isDeleted: { $ne: true },
        'reservations.reservationId': req.params.id
      }).select('reservations').lean();
      const current = currentResource?.reservations?.find((item) => item.reservationId === req.params.id);
      if (!current) return res.status(404).json({ error: 'Reservation not found' });
      if (current.status !== 'PENDING') {
        return res.status(409).json({ error: `Cannot transition reservation from ${current.status} to ${nextStatus}` });
      }
      const event = makeOutboxEvent(
        nextStatus === 'APPROVED' ? 'RESOURCE_APPROVED' : 'RESOURCE_REJECTED',
        {
          resourceId: current.resourceId,
          reservationId: current.reservationId,
          studentId: current.studentId,
          status: nextStatus
        }
      );
      const updated = await Resource.findOneAndUpdate(withOutboxCapacity({
        department,
        isDeleted: { $ne: true },
        'reservations': { $elemMatch: { reservationId: req.params.id, status: 'PENDING' } },
        ...(nextStatus === 'REJECTED' ? { reservedQuantity: { $gte: 1 } } : {})
      }), withResourceMetadata({
        $set: { 'reservations.$.status': nextStatus, 'reservations.$.updatedAt': new Date() },
        ...(nextStatus === 'REJECTED' ? { $inc: { reservedQuantity: -1, availableQuantity: 1 } } : {})
      }, event), { new: true, runValidators: true }).lean();
      if (!updated) {
        if (await isOutboxAtCapacity(current.resourceId)) return res.status(503).json({ error: 'Resource event outbox is full; retry after delivery recovery' });
        return res.status(409).json({ error: 'Reservation changed concurrently; reload and retry' });
      }
      observeResourceClock(updated);
      const reservation = updated.reservations.find((item) => item.reservationId === req.params.id);
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.json({ reservation, resource: toResourcePayload(updated), eventDelivery });
    } catch (error) {
      return next(error);
    }
  });

  app.post('/internal/reservations/:id/cancel', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['STUDENT', 'DEPARTMENT_ADMIN']);
      if (!actor) return;
      const reservationResource = await Resource.findOne({
        department,
        isDeleted: { $ne: true },
        'reservations.reservationId': req.params.id
      }).select('resourceId reservations').lean();
      const current = reservationResource?.reservations?.find((item) => item.reservationId === req.params.id);
      if (!current) return res.status(404).json({ error: 'Reservation not found' });
      if (actor.role === 'STUDENT' && current.studentId !== actor.userId) {
        return res.status(403).json({ error: 'You may only cancel your own reservation' });
      }
      if (!['PENDING', 'APPROVED'].includes(current.status)) {
        return res.status(409).json({ error: `Cannot cancel a ${current.status.toLowerCase()} reservation` });
      }
      const event = makeOutboxEvent('RESOURCE_CANCELLED', {
        resourceId: reservationResource.resourceId,
        reservationId: current.reservationId,
        studentId: current.studentId,
        status: 'CANCELLED'
      });
      const updated = await Resource.findOneAndUpdate(withOutboxCapacity({
        resourceId: reservationResource.resourceId,
        department,
        isDeleted: { $ne: true },
        reservations: {
          $elemMatch: {
            reservationId: req.params.id,
            status: { $in: ['PENDING', 'APPROVED'] },
            ...(actor.role === 'STUDENT' ? { studentId: actor.userId } : {})
          }
        },
        reservedQuantity: { $gte: 1 }
      }), withResourceMetadata({
        $inc: { reservedQuantity: -1, availableQuantity: 1 },
        $set: { 'reservations.$.status': 'CANCELLED', 'reservations.$.updatedAt': new Date() }
      }, event), { new: true, runValidators: true }).lean();
      if (!updated) {
        if (await isOutboxAtCapacity(reservationResource.resourceId)) return res.status(503).json({ error: 'Resource event outbox is full; retry after delivery recovery' });
        return res.status(409).json({ error: 'Reservation changed concurrently; reload and retry' });
      }
      observeResourceClock(updated);
      const reservation = updated.reservations.find((item) => item.reservationId === req.params.id);
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.json({ reservation, resource: toResourcePayload(updated), eventDelivery });
    } catch (error) {
      return next(error);
    }
  });

  app.get('/internal/borrow-records', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['STUDENT', 'DEPARTMENT_ADMIN']);
      if (!actor) return;
      if (actor.role === 'STUDENT' && req.query.studentId !== actor.userId) {
        return res.status(403).json({ error: 'Students may only list their own borrow records' });
      }
      const matching = actor.role === 'STUDENT' ? { 'borrowRecords.studentId': actor.userId } : {};
      const resources = await Resource.find({ department, isDeleted: { $ne: true }, ...matching }).select('borrowRecords').lean();
      const records = resources.flatMap((item) => item.borrowRecords || [])
        .filter((item) => actor.role === 'DEPARTMENT_ADMIN' || item.studentId === actor.userId);
      return res.json(records);
    } catch (error) {
      return next(error);
    }
  });

  app.post('/internal/resources/:id/borrow', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['STUDENT', 'DEPARTMENT_ADMIN']);
      if (!actor) return;
      const reservationId = req.body?.reservationId;
      const candidate = await Resource.findOne({
        resourceId: req.params.id,
        department,
        isDeleted: { $ne: true },
        reservations: { $elemMatch: { reservationId, status: 'APPROVED' } }
      }).select('reservations').lean();
      const approved = candidate?.reservations?.find((item) => item.reservationId === reservationId);
      if (!candidate || !approved) {
        const resourceExists = await Resource.exists({ resourceId: req.params.id, department, isDeleted: { $ne: true } });
        if (!resourceExists) return res.status(404).json({ error: 'Resource not found in this department' });
        return res.status(409).json({ error: 'Borrowing requires an approved reservation' });
      }
      if (actor.role === 'STUDENT' && approved.studentId !== actor.userId) {
        return res.status(403).json({ error: 'You may only borrow against your own reservation' });
      }
      const record = {
        borrowRecordId: createId('borrow'),
        resourceId: req.params.id,
        department,
        reservationId,
        studentId: approved.studentId,
        borrowedAt: new Date(),
        returnedAt: null,
        status: 'BORROWED'
      };
      const event = makeOutboxEvent('RESOURCE_BORROWED', {
        resourceId: req.params.id,
        reservationId,
        borrowRecordId: record.borrowRecordId,
        studentId: record.studentId
      });
      const updated = await Resource.findOneAndUpdate(withOutboxCapacity({
        resourceId: req.params.id,
        department,
        isDeleted: { $ne: true },
        reservedQuantity: { $gte: 1 },
        reservations: { $elemMatch: { reservationId, status: 'APPROVED', studentId: approved.studentId } },
        $expr: { $eq: ['$totalQuantity', { $add: ['$availableQuantity', '$reservedQuantity', '$borrowedQuantity'] }] }
      }), withResourceMetadata({
        $inc: { reservedQuantity: -1, borrowedQuantity: 1 },
        $set: { 'reservations.$.status': 'FULFILLED', 'reservations.$.updatedAt': new Date() },
        $push: { borrowRecords: record }
      }, event), { new: true, runValidators: true }).lean();
      if (!updated) {
        if (await isOutboxAtCapacity(req.params.id)) return res.status(503).json({ error: 'Resource event outbox is full; retry after delivery recovery' });
        return res.status(409).json({ error: 'Reservation changed or quantity unavailable; reload and retry' });
      }
      observeResourceClock(updated);
      const reservation = updated.reservations.find((item) => item.reservationId === reservationId);
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.status(201).json({
        borrowRecord: record,
        reservation,
        resource: toResourcePayload(updated),
        eventDelivery
      });
    } catch (error) {
      return next(error);
    }
  });

  app.post('/internal/resources/:id/return', requireInternalService, async (req, res, next) => {
    try {
      const actor = authorizeLifecycleActor(req, res, ['STUDENT', 'DEPARTMENT_ADMIN']);
      if (!actor) return;
      const borrowRecordId = req.body?.borrowRecordId;
      const candidate = await Resource.findOne({
        resourceId: req.params.id,
        department,
        isDeleted: { $ne: true },
        'borrowRecords.borrowRecordId': borrowRecordId
      }).select('borrowRecords').lean();
      const current = candidate?.borrowRecords?.find((item) => item.borrowRecordId === borrowRecordId);
      if (!candidate || !current) return res.status(404).json({ error: 'Borrow record not found for this resource' });
      if (actor.role === 'STUDENT' && current.studentId !== actor.userId) {
        return res.status(403).json({ error: 'You may only return your own borrowed resource' });
      }
      if (current.status !== 'BORROWED') return res.status(409).json({ error: 'Borrow record has already been returned' });
      const event = makeOutboxEvent('RESOURCE_RETURNED', {
        resourceId: req.params.id,
        borrowRecordId,
        reservationId: current.reservationId,
        studentId: current.studentId
      });
      const updated = await Resource.findOneAndUpdate(withOutboxCapacity({
        resourceId: req.params.id,
        department,
        isDeleted: { $ne: true },
        borrowedQuantity: { $gte: 1 },
        borrowRecords: {
          $elemMatch: {
            borrowRecordId,
            status: 'BORROWED',
            ...(actor.role === 'STUDENT' ? { studentId: actor.userId } : {})
          }
        },
        $expr: { $eq: ['$totalQuantity', { $add: ['$availableQuantity', '$reservedQuantity', '$borrowedQuantity'] }] }
      }), withResourceMetadata({
        $inc: { borrowedQuantity: -1, availableQuantity: 1 },
        $set: { 'borrowRecords.$.status': 'RETURNED', 'borrowRecords.$.returnedAt': new Date() }
      }, event), { new: true, runValidators: true }).lean();
      if (!updated) {
        if (await isOutboxAtCapacity(req.params.id)) return res.status(503).json({ error: 'Resource event outbox is full; retry after delivery recovery' });
        return res.status(409).json({ error: 'Borrow record changed or quantity unavailable; reload and retry' });
      }
      observeResourceClock(updated);
      const record = updated.borrowRecords.find((item) => item.borrowRecordId === borrowRecordId);
      const eventDelivery = await tryDeliverOutboxEvent(event.eventId);
      return res.json({ borrowRecord: record, resource: toResourcePayload(updated), eventDelivery });
    } catch (error) {
      return next(error);
    }
  });

  app.post('/internal/heartbeat', (req, res) => {
    const body = req.body || {};
    lastHeartbeat = nowISO();
    nodeHealth = body.status === 'healthy' ? 'healthy' : 'unknown';
    res.json({ ok: true, department, status: nodeHealth, lastHeartbeat });
  });

  app.use((error, _req, res, _next) => {
    console.error(`${department} request failed: ${error.message}`);
    return res.status(503).json({ error: 'Department service database operation failed' });
  });

  const protoPath = fileURLToPath(new URL('../protos/department.proto', import.meta.url));
  const packageDefinition = protoLoader.loadSync(protoPath, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
  const proto = grpc.loadPackageDefinition(packageDefinition).campus;

  const grpcServer = new grpc.Server();
  grpcServer.addService(proto.DepartmentService.service, {
    SearchResource: (call, callback) => {
      const q = (call.request?.name || '').toLowerCase();
      const escapedQuery = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      Resource.find({
        department,
        isDeleted: { $ne: true },
        $or: [
          { name: { $regex: escapedQuery, $options: 'i' } },
          { category: { $regex: escapedQuery, $options: 'i' } },
          { description: { $regex: escapedQuery, $options: 'i' } }
        ]
      }).lean().then((matches) => {
        callback(null, { resources: matches.map((item) => toResourcePayload(item)) });
      }).catch((error) => {
        console.error(`${department} gRPC search failed: ${error.message}`);
        callback({ code: grpc.status.UNAVAILABLE, message: 'Department database unavailable' });
      });
    },
    GetHealth: (_call, callback) => callback(null, { status: nodeHealth, department, nodeId: department }),
    TriggerElection: (_call, callback) => {
      bullyElection.startElection('grpc-trigger')
        .then((state) => callback(null, {
          winner: state.coordinator || '',
          department,
          success: Boolean(state.coordinator),
          electionStatus: state.electionStatus
        }))
        .catch((error) => callback({ code: grpc.status.UNAVAILABLE, message: error.message }));
    }
  });

  const httpServer = http.createServer(app);
  const initialize = async (uri = mongoUri) => {
    await connectDatabase(uri, `${department} service`);
    await Resource.init();
    const count = await Resource.countDocuments({ department });
    const seeded = seedResources(department, 5);
    await Resource.bulkWrite(seeded.map((resource) => ({
      updateOne: {
        filter: { resourceId: resource.resourceId, department },
        update: { $setOnInsert: resource },
        upsert: true
      }
    })), { ordered: true });
    if (count === 0) console.log(`${department} initialized ${seeded.length} development resources`);
    outboxPublisher = createOutboxPublisher({ Resource, department });
    await outboxPublisher.start();
  };

  const start = async () => {
    await initialize();
    httpServer.listen(httpPort, () => {
      console.log(`${department} service listening on ${httpPort}`);
    });
    const reportHeartbeat = async () => {
      try {
        const response = await fetch(`${process.env.GATEWAY_INTERNAL_URL || `http://localhost:${gatewayPort}`}/internal/heartbeat`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Internal-Service-Token': internalServiceToken
          },
          body: JSON.stringify({ department, nodeId: department, status: 'healthy' }),
          signal: AbortSignal.timeout(peerTimeoutMs)
        });
        if (!response.ok) throw new Error(`Gateway heartbeat failed (${response.status})`);
        lastHeartbeat = nowISO();
      } catch (error) {
        console.warn(`${department} gateway heartbeat failed: ${error.message}`);
      }
    };
    await reportHeartbeat();
    heartbeatTimer = setInterval(reportHeartbeat, Number(process.env.HEARTBEAT_INTERVAL_MS) || 5000);
    heartbeatTimer.unref();
    electionStartupTimer = setTimeout(() => {
      bullyElection.checkPeers().catch((error) => {
        bullyElection.electionStatus = 'failed';
        bullyElection.latestResult = { success: false, error: error.message };
      });
    }, Number(process.env.ELECTION_STARTUP_DELAY_MS) || 500);
    electionMonitor = setInterval(() => {
      bullyElection.checkPeers().catch((error) => {
        bullyElection.electionStatus = 'failed';
        bullyElection.latestResult = { success: false, error: error.message };
      });
    }, Number(process.env.ELECTION_MONITOR_INTERVAL_MS) || 5000);
    electionMonitor.unref();
    grpcServer.bindAsync(`0.0.0.0:${grpcPort}`, grpc.ServerCredentials.createInsecure(), (error) => {
      if (error) {
        console.error(`${department} gRPC error: ${error.message}`);
        return;
      }
      grpcServer.start();
      console.log(`${department} gRPC service listening on ${grpcPort}`);
    });
  };

  const stop = async () => {
    if (electionStartupTimer) clearTimeout(electionStartupTimer);
    if (electionMonitor) clearInterval(electionMonitor);
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    await new Promise((resolve) => {
      if (!httpServer.listening) return resolve();
      httpServer.close(resolve);
      httpServer.closeAllConnections();
    });
    grpcServer.forceShutdown();
    if (outboxPublisher) await outboxPublisher.stop();
    await disconnectDatabase();
  };

  return {
    app,
    start,
    stop,
    initialize,
    getState: () => ({
      department,
      coordinator: bullyElection.getState().coordinator,
      lamportClock: lamportClock.getTimestamp(),
      vectorClock,
      lastHeartbeat,
      status: nodeHealth,
      election: bullyElection.getState()
    })
  };
}
