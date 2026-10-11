import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import request from 'supertest';
import { spawn } from 'node:child_process';
import WebSocket from 'ws';
import mongoose from 'mongoose';
import { app, initializeGateway, server, events, wss } from '../gateway/server.js';
import { createResource } from '../shared/store.js';
import { createOutboxEntry, createOutboxPublisher } from '../shared/outboxPublisher.js';
import { createResourceModel } from '../services/models/Resource.js';

const services = [];
const outboxMongoose = new mongoose.Mongoose();
let userSequence = 0;
const testRunId = `${process.pid}_${Date.now()}`;
const liveRabbitUrl = process.env.RABBITMQ_URL || 'amqp://127.0.0.1:5672';
const gatewayPort = Number(process.env.GATEWAY_PORT || 4000);
const departmentPorts = {
  CS: Number(process.env.CS_PORT || 5001),
  IT: Number(process.env.IT_PORT || 5002),
  ENTC: Number(process.env.ENTC_PORT || 5003)
};
const grpcPorts = {
  CS: Number(process.env.CS_GRPC_PORT || 6001),
  IT: Number(process.env.IT_GRPC_PORT || 6002),
  ENTC: Number(process.env.ENTC_GRPC_PORT || 6003)
};
const secondaryCsPort = Number(process.env.TEST_SECONDARY_CS_PORT || 5011);
const secondaryCsGrpcPort = Number(process.env.TEST_SECONDARY_CS_GRPC_PORT || 6011);
const testDatabaseUri = (name) => `mongodb://127.0.0.1:27017/campus_share_test_${name}_${testRunId}`;
const testDatabaseUris = ['auth', 'cs', 'it', 'entc', 'outbox'].map(testDatabaseUri);
let outboxConnection;
let OutboxResource;
let csConnection;
let CSResource;
let outboxFixtureSequence = 0;

const newEmail = (label) => `${label}-${Date.now()}-${userSequence += 1}@example.test`;

function startService(file, env) {
  const child = spawn(process.execPath, [file], { env: { ...process.env, ...env }, stdio: 'ignore' });
  services.push({ child, file, env });
  return child;
}

async function waitFor(url, timeoutMs = 20000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch (_error) {
      // retry until service is available
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

beforeAll(async () => {
  process.env.ADMIN_REGISTRATION_KEY = 'test-admin-registration-key';
  process.env.RABBITMQ_URL = 'amqp://127.0.0.1:1';
  process.env.CS_MONGO_URI = testDatabaseUris[1];
  process.env.IT_MONGO_URI = testDatabaseUris[2];
  process.env.ENTC_MONGO_URI = testDatabaseUris[3];
  outboxConnection = await outboxMongoose.createConnection(testDatabaseUris[4]).asPromise();
  OutboxResource = createResourceModel(outboxConnection);
  await OutboxResource.init();
  csConnection = await outboxMongoose.createConnection(testDatabaseUris[1]).asPromise();
  CSResource = createResourceModel(csConnection);
  await CSResource.init();
  await initializeGateway(testDatabaseUris[0]);
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(gatewayPort, resolve);
  });
  startService('services/cs/server.js', {
    CS_PORT: String(departmentPorts.CS),
    CS_GRPC_PORT: String(grpcPorts.CS),
    GATEWAY_PORT: String(gatewayPort),
    GATEWAY_INTERNAL_URL: `http://localhost:${gatewayPort}`
  });
  startService('services/it/server.js', {
    IT_PORT: String(departmentPorts.IT),
    IT_GRPC_PORT: String(grpcPorts.IT),
    GATEWAY_PORT: String(gatewayPort),
    GATEWAY_INTERNAL_URL: `http://localhost:${gatewayPort}`
  });
  startService('services/entc/server.js', {
    ENTC_PORT: String(departmentPorts.ENTC),
    ENTC_GRPC_PORT: String(grpcPorts.ENTC),
    GATEWAY_PORT: String(gatewayPort),
    GATEWAY_INTERNAL_URL: `http://localhost:${gatewayPort}`
  });

  await Promise.all([
    ...Object.values(departmentPorts).map((port) => waitFor(`http://localhost:${port}/internal/health`))
  ]);
}, 30000);

afterAll(async () => {
  const activeServices = services.filter(({ child }) => child.exitCode === null && child.signalCode === null);
  const exits = activeServices.map(({ child, file }) => new Promise((resolve) => {
    child.once('exit', resolve);
    child.kill('SIGTERM');
  }));
  await Promise.all(exits);
  wss.clients.forEach((client) => client.terminate());
  await new Promise((resolve) => server.close(resolve));
  if (mongoose.connection.readyState !== 0) await mongoose.disconnect();
  if (outboxConnection?.readyState !== 0) await outboxConnection.close();
  if (csConnection?.readyState !== 0) await csConnection.close();
  for (const uri of testDatabaseUris) {
    const connection = await mongoose.createConnection(uri).asPromise();
    await connection.dropDatabase();
    await connection.close();
  }
}, 60000);

async function registerUser({ role = 'STUDENT', department, label = 'user' } = {}) {
  const payload = {
    name: `Test ${label}`,
    email: newEmail(label),
    password: 'Password123',
    role,
    ...(department ? { department } : {}),
    ...(role === 'DEPARTMENT_ADMIN' ? { adminRegistrationKey: 'test-admin-registration-key' } : {})
  };
  const response = await request(app).post('/api/auth/register').send(payload);
  expect(response.status).toBe(201);
  return { token: response.body.token, user: response.body.user };
}

async function createSingleUnitResource(adminToken, name = `Lifecycle ${Date.now()}`) {
  const response = await request(app)
    .post('/api/resources')
    .set('Authorization', `Bearer ${adminToken}`)
    .send({ name, category: 'Test equipment', laboratory: 'Test Lab', totalQuantity: 1 });
  expect(response.status).toBe(201);
  expect(response.body.availableQuantity).toBe(1);
  return response.body;
}

async function reserve(token, resourceId) {
  return request(app)
    .post('/api/reservations')
    .set('Authorization', `Bearer ${token}`)
    .send({ resourceId });
}

async function getResource(resourceId) {
  return request(app).get(`/api/resources/${encodeURIComponent(resourceId)}`);
}

function expectQuantityInvariant(resource) {
  expect(resource.availableQuantity).toBeGreaterThanOrEqual(0);
  expect(resource.reservedQuantity).toBeGreaterThanOrEqual(0);
  expect(resource.borrowedQuantity).toBeGreaterThanOrEqual(0);
  expect(resource.totalQuantity).toBe(
    resource.availableQuantity + resource.reservedQuantity + resource.borrowedQuantity
  );
}

async function createOutboxFixture(eventType = 'RESOURCE_RESERVED', options = {}) {
  const resource = createResource({
    department: 'CS',
    name: `Outbox ${testRunId} ${outboxFixtureSequence += 1}`,
    category: 'Test',
    totalQuantity: 1,
    availableQuantity: 0,
    reservedQuantity: 1
  });
  const event = createOutboxEntry('CS', eventType, {
    resourceId: resource.resourceId,
    reservationId: `reservation-${outboxFixtureSequence}`
  });
  await OutboxResource.create({ ...resource, ...options, outbox: [event] });
  return { resourceId: resource.resourceId, event };
}

function createTestPublisher(publishRabbit, options = {}) {
  return createOutboxPublisher({
    Resource: OutboxResource,
    department: 'CS',
    publishRabbit,
    relayEvent: async () => 'accepted_no_clients',
    retryBaseMs: 0,
    retryMaxMs: 0,
    completedRetentionMs: 0,
    log: { info() {}, warn() {}, error() {} },
    ...options
  });
}

describe('Campus Share API smoke tests', () => {
  it('does not allow unauthenticated self-assignment of an admin role', async () => {
    const response = await request(app)
      .post('/api/auth/register')
      .send({
        name: 'Unauthorized Admin',
        email: newEmail('unauthorized-admin'),
        password: 'Password123',
        role: 'DEPARTMENT_ADMIN',
        department: 'CS',
        adminRegistrationKey: 'not-the-configured-key'
      });
    expect(response.status).toBe(403);
  });

  it('registers and logs in a student user', async () => {
    const email = newEmail('auth');
    const response = await request(app)
      .post('/api/auth/register')
      .send({ name: 'Test User', email, password: 'Password123', role: 'STUDENT' });

    expect(response.status).toBe(201);
    expect(response.body.user.email).toBe(email);
    expect(response.body.token).toBeTruthy();

    const login = await request(app)
      .post('/api/auth/login')
      .send({ email, password: 'Password123' });

    expect(login.status).toBe(200);
    expect(login.body.user.role).toBe('STUDENT');
  });

  it('keeps gateway accounts after reconnecting to MongoDB', async () => {
    const student = await registerUser({ label: 'auth-persistence' });
    await mongoose.disconnect();
    await initializeGateway(testDatabaseUris[0]);

    const login = await request(app)
      .post('/api/auth/login')
      .send({ email: student.user.email, password: 'Password123' });
    expect(login.status).toBe(200);
    expect(login.body.user.userId).toBe(student.user.userId);
  });

  it('fails department initialization clearly when MongoDB is unavailable', async () => {
    const moduleUrl = new URL('../services/baseService.js', import.meta.url).href;
    const script = `
      const { createDepartmentService } = await import(${JSON.stringify(moduleUrl)});
      const service = createDepartmentService({ department: 'CS', httpPort: 0, grpcPort: 0 });
      try {
        await service.initialize('mongodb://127.0.0.1:1/campus_share_unavailable_test');
        process.exit(2);
      } catch {
        process.exit(0);
      }
    `;
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], {
      env: { ...process.env, MONGO_SERVER_SELECTION_TIMEOUT_MS: '300' },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let output = '';
    child.stdout.on('data', (chunk) => { output += chunk.toString(); });
    child.stderr.on('data', (chunk) => { output += chunk.toString(); });
    const exitCode = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', resolve);
    });
    expect(exitCode).toBe(0);
    expect(output).toContain('CS service MongoDB connection failed');
  });

  it('returns aggregated search results from all departments', async () => {
    const response = await request(app).get('/api/resources/search?name=Arduino');
    expect(response.status).toBe(200);
    expect(response.body.query).toBe('Arduino');
    expect(response.body.total).toBeGreaterThan(0);
    expect(response.body.departments.CS).toBeDefined();
    expect(response.body.departments.IT).toBeDefined();
    expect(response.body.departments.ENTC).toBeDefined();
  });

  it('reserves an available unit and emits a structured lifecycle event', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'reserve-admin' });
    const student = await registerUser({ label: 'reserve-student' });
    const resource = await createSingleUnitResource(admin.token);
    const before = new Set(events.map((event) => event.eventId));
    const socket = new WebSocket(`ws://localhost:${gatewayPort}/ws/updates`);
    await new Promise((resolve, reject) => {
      socket.once('open', resolve);
      socket.once('error', reject);
    });
    const websocketEvent = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timed out waiting for the reservation WebSocket event')), 3000);
      socket.on('message', (raw) => {
        const payload = JSON.parse(raw.toString());
        if (payload.type === 'EVENT' && payload.event.eventType === 'RESOURCE_RESERVED' && payload.event.resourceId === resource.resourceId) {
          clearTimeout(timer);
          resolve(payload.event);
        }
      });
    });

    const response = await reserve(student.token, resource.resourceId);
    const receivedEvent = await websocketEvent;
    socket.close();
    expect(response.status).toBe(201);
    expect(response.body.reservation).toMatchObject({
      resourceId: resource.resourceId,
      department: 'CS',
      studentId: student.user.userId,
      status: 'PENDING'
    });
    expectQuantityInvariant(response.body.resource);
    expect(response.body.resource.availableQuantity).toBe(0);
    expect(response.body.resource.reservedQuantity).toBe(1);
    expect(response.body.eventDelivery.websocketRelay).toBe('relayed');
    expect(response.body.eventDelivery.rabbitmq).toBe('pending');

    const event = events.find((item) => !before.has(item.eventId) && item.eventType === 'RESOURCE_RESERVED');
    expect(event).toMatchObject({
      eventType: 'RESOURCE_RESERVED',
      sourceDepartment: 'CS',
      resourceId: resource.resourceId,
      reservationId: response.body.reservation.reservationId,
      lamportTimestamp: expect.any(Number)
    });
    expect(event.timestamp).toBeTruthy();
    expect(Number.isSafeInteger(event.lamportTimestamp)).toBe(true);
    expect(event.lamportTimestamp).toBeGreaterThanOrEqual(0);
    expect(receivedEvent.eventId).toBe(event.eventId);
    const stored = await CSResource.findOne({ resourceId: resource.resourceId }).lean();
    expect(stored.outbox).toContainEqual(expect.objectContaining({
      eventId: event.eventId,
      eventType: 'RESOURCE_RESERVED',
      lamportTimestamp: event.lamportTimestamp,
      rabbitmqStatus: 'pending',
      websocketRelayStatus: 'relayed'
    }));
  });

  it('atomically allows only one simultaneous reservation of the last unit', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'race-admin' });
    const students = await Promise.all([
      registerUser({ label: 'race-student-a' }),
      registerUser({ label: 'race-student-b' })
    ]);
    const resource = await createSingleUnitResource(admin.token);

    const results = await Promise.all(students.map((student) => reserve(student.token, resource.resourceId)));
    expect(results.map((result) => result.status).sort()).toEqual([201, 409]);
    const current = await getResource(resource.resourceId);
    expectQuantityInvariant(current.body);
    expect(current.body.availableQuantity).toBe(0);
    expect(current.body.reservedQuantity).toBe(1);
  });

  it('serializes last-unit reservations across two department processes', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'multi-process-admin' });
    const students = await Promise.all([
      registerUser({ label: 'multi-process-a' }),
      registerUser({ label: 'multi-process-b' })
    ]);
    const resource = await createSingleUnitResource(admin.token);
    const token = process.env.INTERNAL_SERVICE_TOKEN || 'campus-share-local-internal-token';
    startService('services/cs/server.js', {
      CS_PORT: String(secondaryCsPort),
      CS_GRPC_PORT: String(secondaryCsGrpcPort),
      GATEWAY_PORT: String(gatewayPort),
      GATEWAY_INTERNAL_URL: `http://localhost:${gatewayPort}`
    });
    await waitFor(`http://localhost:${secondaryCsPort}/internal/health`);

    const beforeEvents = events.filter((event) => event.eventType === 'RESOURCE_RESERVED'
      && event.resourceId === resource.resourceId).length;
    const results = await Promise.all(students.map((student, index) => fetch(
      `http://localhost:${index === 0 ? departmentPorts.CS : secondaryCsPort}/internal/resources/${resource.resourceId}/reservations`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Service-Token': token,
          'X-Actor-Id': student.user.userId,
          'X-Actor-Role': 'STUDENT'
        },
        body: '{}'
      }
    )));
    expect(results.map((response) => response.status).sort()).toEqual([201, 409]);

    const current = await getResource(resource.resourceId);
    expectQuantityInvariant(current.body);
    expect(current.body.availableQuantity).toBe(0);
    expect(current.body.reservedQuantity).toBe(1);
    expect(events.filter((event) => event.eventType === 'RESOURCE_RESERVED'
      && event.resourceId === resource.resourceId)).toHaveLength(beforeEvents + 1);
  });

  it('keeps resource and reservation records after restarting the owning service', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'restart-admin' });
    const student = await registerUser({ label: 'restart-student' });
    const resource = await createSingleUnitResource(admin.token);
    const created = await reserve(student.token, resource.resourceId);
    const reservationEventId = created.body.eventDelivery.eventId;

    const csService = [...services].reverse().find(({ file }) => file === 'services/cs/server.js');
    const stopped = new Promise((resolve) => csService.child.once('exit', resolve));
    csService.child.kill('SIGTERM');
    await stopped;

    const restarted = startService(csService.file, csService.env);
    expect(restarted.pid).toBeTruthy();
    await waitFor(`http://localhost:${departmentPorts.CS}/internal/health`);
    const persistedResource = await getResource(resource.resourceId);
    expect(persistedResource.status).toBe(200);
    expectQuantityInvariant(persistedResource.body);
    expect(persistedResource.body.availableQuantity).toBe(0);
    expect(persistedResource.body.reservedQuantity).toBe(1);
    const stored = await CSResource.findOne({ resourceId: resource.resourceId }).lean();
    expect(stored.outbox).toContainEqual(expect.objectContaining({
      eventId: reservationEventId,
      eventType: 'RESOURCE_RESERVED',
      rabbitmqStatus: 'pending'
    }));

    const myReservations = await request(app)
      .get('/api/reservations/my')
      .set('Authorization', `Bearer ${student.token}`);
    expect(myReservations.status).toBe(200);
    expect(myReservations.body.reservations).toContainEqual(expect.objectContaining({
      reservationId: created.body.reservation.reservationId,
      resourceId: resource.resourceId,
      status: 'PENDING'
    }));
  });

  it('rejects a reservation when no quantity is available without publishing a success event', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'IT', label: 'empty-admin' });
    const firstStudent = await registerUser({ label: 'empty-student-a' });
    const secondStudent = await registerUser({ label: 'empty-student-b' });
    const resource = await createSingleUnitResource(admin.token, `Unavailable ${Date.now()}`);
    const first = await reserve(firstStudent.token, resource.resourceId);
    expect(first.status).toBe(201);
    const eventIds = new Set(events.map((event) => event.eventId));

    const failed = await reserve(secondStudent.token, resource.resourceId);
    expect(failed.status).toBe(409);
    expect(events.filter((event) => !eventIds.has(event.eventId) && event.eventType === 'RESOURCE_RESERVED')).toHaveLength(0);
    const current = await getResource(resource.resourceId);
    expectQuantityInvariant(current.body);
    expect(current.body.availableQuantity).toBe(0);
    expect(current.body.reservedQuantity).toBe(1);
  });

  it('does not record an outbox success event after an exhausted reservation fails', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'outbox-empty-admin' });
    const firstStudent = await registerUser({ label: 'outbox-empty-a' });
    const secondStudent = await registerUser({ label: 'outbox-empty-b' });
    const resource = await createSingleUnitResource(admin.token);
    expect((await reserve(firstStudent.token, resource.resourceId)).status).toBe(201);

    const before = await CSResource.findOne({ resourceId: resource.resourceId }).lean();
    const failed = await reserve(secondStudent.token, resource.resourceId);
    const after = await CSResource.findOne({ resourceId: resource.resourceId }).lean();

    expect(failed.status).toBe(409);
    expect(after.outbox.filter((entry) => entry.eventType === 'RESOURCE_RESERVED'))
      .toHaveLength(before.outbox.filter((entry) => entry.eventType === 'RESOURCE_RESERVED').length);
    expectQuantityInvariant(await getResource(resource.resourceId).then((response) => response.body));
  });

  it('cancels a reservation and restores availability', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'ENTC', label: 'cancel-admin' });
    const student = await registerUser({ label: 'cancel-student' });
    const resource = await createSingleUnitResource(admin.token);
    const created = await reserve(student.token, resource.resourceId);
    const beforeCancel = new Set(events.map((event) => event.eventId));
    const response = await request(app)
      .delete(`/api/reservations/${created.body.reservation.reservationId}`)
      .set('Authorization', `Bearer ${student.token}`);

    expect(response.status).toBe(200);
    expect(response.body.reservation.status).toBe('CANCELLED');
    expectQuantityInvariant(response.body.resource);
    expect(response.body.resource.availableQuantity).toBe(1);
    expect(response.body.resource.reservedQuantity).toBe(0);
    expect(events.some((event) => !beforeCancel.has(event.eventId)
      && event.eventType === 'RESOURCE_CANCELLED'
      && event.reservationId === created.body.reservation.reservationId)).toBe(true);
    const beforeDuplicateCancelCount = events.filter((event) => event.eventType === 'RESOURCE_CANCELLED').length;
    const duplicate = await request(app)
      .delete(`/api/reservations/${created.body.reservation.reservationId}`)
      .set('Authorization', `Bearer ${student.token}`);
    expect(duplicate.status).toBe(409);
    expect(events.filter((event) => event.eventType === 'RESOURCE_CANCELLED')).toHaveLength(beforeDuplicateCancelCount);
  });

  it('prevents students from viewing or cancelling another student reservation', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'privacy-admin' });
    const owner = await registerUser({ label: 'reservation-owner' });
    const stranger = await registerUser({ label: 'reservation-stranger' });
    const resource = await createSingleUnitResource(admin.token);
    const created = await reserve(owner.token, resource.resourceId);
    const reservationId = created.body.reservation.reservationId;

    const details = await request(app)
      .get(`/api/reservations/${reservationId}`)
      .set('Authorization', `Bearer ${stranger.token}`);
    const cancel = await request(app)
      .delete(`/api/reservations/${reservationId}`)
      .set('Authorization', `Bearer ${stranger.token}`);
    expect(details.status).toBe(403);
    expect(cancel.status).toBe(403);
  });

  it('enforces department ownership for administrators', async () => {
    const csAdmin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'ownership-cs-admin' });
    const itAdmin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'IT', label: 'ownership-it-admin' });
    const resource = await createSingleUnitResource(csAdmin.token);

    const update = await request(app)
      .put(`/api/resources/${resource.resourceId}`)
      .set('Authorization', `Bearer ${itAdmin.token}`)
      .send({ name: 'Unauthorized change' });
    expect(update.status).toBe(403);
    const current = await getResource(resource.resourceId);
    expect(current.body.name).toBe(resource.name);

    const internalToken = process.env.INTERNAL_SERVICE_TOKEN || 'campus-share-local-internal-token';
    const directWrite = await fetch(`http://localhost:${departmentPorts.CS}/internal/resources`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Service-Token': internalToken,
        'X-Actor-Id': itAdmin.user.userId,
        'X-Actor-Role': 'DEPARTMENT_ADMIN',
        'X-Actor-Department': 'IT'
      },
      body: JSON.stringify({ name: 'Cross-department injection', category: 'Test' })
    });
    expect(directWrite.status).toBe(403);
  });

  it('requires approval to borrow, then supports one return only', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'borrow-admin' });
    const student = await registerUser({ label: 'borrow-student' });
    const stranger = await registerUser({ label: 'return-stranger' });
    const resource = await createSingleUnitResource(admin.token);
    const created = await reserve(student.token, resource.resourceId);
    const reservationId = created.body.reservation.reservationId;
    const pendingHistory = await request(app)
      .get('/api/reservations/my')
      .set('Authorization', `Bearer ${student.token}`);
    expect(pendingHistory.status).toBe(200);
    expect(pendingHistory.body.reservations).toContainEqual(expect.objectContaining({
      reservationId,
      studentId: student.user.userId,
      status: 'PENDING'
    }));

    const earlyBorrow = await request(app)
      .post(`/api/resources/${resource.resourceId}/borrow`)
      .set('Authorization', `Bearer ${student.token}`)
      .send({ reservationId });
    expect(earlyBorrow.status).toBe(409);
    const earlyEventCount = events.filter((event) => event.eventType === 'RESOURCE_BORROWED').length;

    const approved = await request(app)
      .patch(`/api/reservations/${reservationId}/status`)
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ status: 'APPROVED' });
    expect(approved.status).toBe(200);

    const borrowed = await request(app)
      .post(`/api/resources/${resource.resourceId}/borrow`)
      .set('Authorization', `Bearer ${student.token}`)
      .send({ reservationId });
    expect(borrowed.status).toBe(201);
    expect(borrowed.body.reservation.status).toBe('FULFILLED');
    expect(borrowed.body.borrowRecord.status).toBe('BORROWED');
    expectQuantityInvariant(borrowed.body.resource);
    expect(borrowed.body.resource.borrowedQuantity).toBe(1);
    const borrowedHistory = await request(app)
      .get('/api/borrow-records/my')
      .set('Authorization', `Bearer ${student.token}`);
    expect(borrowedHistory.status).toBe(200);
    expect(borrowedHistory.body.borrowRecords).toContainEqual(expect.objectContaining({
      borrowRecordId: borrowed.body.borrowRecord.borrowRecordId,
      studentId: student.user.userId,
      status: 'BORROWED'
    }));
    const strangerHistory = await request(app)
      .get('/api/borrow-records/my')
      .set('Authorization', `Bearer ${stranger.token}`);
    expect(strangerHistory.status).toBe(200);
    expect(strangerHistory.body.borrowRecords).not.toContainEqual(expect.objectContaining({
      borrowRecordId: borrowed.body.borrowRecord.borrowRecordId
    }));
    expect(events.filter((event) => event.eventType === 'RESOURCE_BORROWED')).toHaveLength(earlyEventCount + 1);
    expect(events.some((event) => event.eventType === 'RESOURCE_BORROWED'
      && event.borrowRecordId === borrowed.body.borrowRecord.borrowRecordId)).toBe(true);
    const repeatedBorrow = await request(app)
      .post(`/api/resources/${resource.resourceId}/borrow`)
      .set('Authorization', `Bearer ${student.token}`)
      .send({ reservationId });
    expect(repeatedBorrow.status).toBe(409);
    expect(events.filter((event) => event.eventType === 'RESOURCE_BORROWED')).toHaveLength(earlyEventCount + 1);

    const unauthorizedReturn = await request(app)
      .post(`/api/resources/${resource.resourceId}/return`)
      .set('Authorization', `Bearer ${stranger.token}`)
      .send({ borrowRecordId: borrowed.body.borrowRecord.borrowRecordId });
    expect(unauthorizedReturn.status).toBe(403);

    const returned = await request(app)
      .post(`/api/resources/${resource.resourceId}/return`)
      .set('Authorization', `Bearer ${student.token}`)
      .send({ borrowRecordId: borrowed.body.borrowRecord.borrowRecordId });
    expect(returned.status).toBe(200);
    expect(returned.body.borrowRecord.status).toBe('RETURNED');
    expectQuantityInvariant(returned.body.resource);
    expect(returned.body.resource.availableQuantity).toBe(1);
    expect(returned.body.resource.borrowedQuantity).toBe(0);
    const returnedHistory = await request(app)
      .get('/api/borrow-records/my')
      .set('Authorization', `Bearer ${student.token}`);
    expect(returnedHistory.status).toBe(200);
    expect(returnedHistory.body.borrowRecords).toContainEqual(expect.objectContaining({
      borrowRecordId: borrowed.body.borrowRecord.borrowRecordId,
      studentId: student.user.userId,
      status: 'RETURNED'
    }));
    const fulfilledHistory = await request(app)
      .get('/api/reservations/my')
      .set('Authorization', `Bearer ${student.token}`);
    expect(fulfilledHistory.status).toBe(200);
    expect(fulfilledHistory.body.reservations).toContainEqual(expect.objectContaining({
      reservationId,
      studentId: student.user.userId,
      status: 'FULFILLED'
    }));
    expect(events.some((event) => event.eventType === 'RESOURCE_RETURNED'
      && event.borrowRecordId === borrowed.body.borrowRecord.borrowRecordId)).toBe(true);
    const returnEventCount = events.filter((event) => event.eventType === 'RESOURCE_RETURNED').length;

    const duplicateReturn = await request(app)
      .post(`/api/resources/${resource.resourceId}/return`)
      .set('Authorization', `Bearer ${student.token}`)
      .send({ borrowRecordId: borrowed.body.borrowRecord.borrowRecordId });
    expect(duplicateReturn.status).toBe(409);
    expect(events.filter((event) => event.eventType === 'RESOURCE_RETURNED')).toHaveLength(returnEventCount);
  });

  it('rejects a department administrator trying to decide another department reservation', async () => {
    const csAdmin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'CS', label: 'state-cs-admin' });
    const itAdmin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'IT', label: 'state-it-admin' });
    const student = await registerUser({ label: 'state-student' });
    const resource = await createSingleUnitResource(csAdmin.token);
    const created = await reserve(student.token, resource.resourceId);
    const result = await request(app)
      .patch(`/api/reservations/${created.body.reservation.reservationId}/status`)
      .set('Authorization', `Bearer ${itAdmin.token}`)
      .send({ status: 'APPROVED' });
    expect(result.status).toBe(404);
    const current = await getResource(resource.resourceId);
    expectQuantityInvariant(current.body);
    expect(current.body.reservedQuantity).toBe(1);
  });

  it('rejects a pending reservation and releases its reserved quantity', async () => {
    const admin = await registerUser({ role: 'DEPARTMENT_ADMIN', department: 'IT', label: 'reject-admin' });
    const student = await registerUser({ label: 'reject-student' });
    const resource = await createSingleUnitResource(admin.token);
    const created = await reserve(student.token, resource.resourceId);

    const rejected = await request(app)
      .patch(`/api/reservations/${created.body.reservation.reservationId}/status`)
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ status: 'REJECTED' });
    expect(rejected.status).toBe(200);
    expect(rejected.body.reservation.status).toBe('REJECTED');
    expectQuantityInvariant(rejected.body.resource);
    expect(rejected.body.resource.availableQuantity).toBe(1);
    expect(rejected.body.resource.reservedQuantity).toBe(0);

    const invalidTransition = await request(app)
      .patch(`/api/reservations/${created.body.reservation.reservationId}/status`)
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ status: 'APPROVED' });
    expect(invalidTransition.status).toBe(409);
    const current = await getResource(resource.resourceId);
    expectQuantityInvariant(current.body);
    expect(current.body.availableQuantity).toBe(1);
  });
});

describe('durable department event outbox', () => {
  it('retries a failed broker publication with the same event ID', async () => {
    const { resourceId, event } = await createOutboxFixture();
    const eventIds = [];
    let brokerAvailable = false;
    const publisher = createTestPublisher(async (published) => {
      eventIds.push(published.eventId);
      if (!brokerAvailable) throw Object.assign(new Error('unavailable'), { code: 'ECONNREFUSED' });
    });

    const first = await publisher.processEvent(event.eventId);
    expect(first.rabbitmq).toBe('pending');
    expect(first.websocketRelay).toBe('accepted_no_clients');
    const pending = await OutboxResource.findOne({ resourceId }).lean();
    expect(pending.outbox[0].rabbitmqStatus).toBe('pending');

    brokerAvailable = true;
    const retried = await publisher.processEvent(event.eventId);
    expect(retried.rabbitmq).toBe('published');
    expect(eventIds).toEqual([event.eventId, event.eventId]);
  });

  it('waits for broker confirmation before recording publication', async () => {
    const { resourceId, event } = await createOutboxFixture();
    let confirm;
    let started;
    const publishStarted = new Promise((resolve) => { started = resolve; });
    const publisher = createTestPublisher(() => {
      started();
      return new Promise((resolve) => { confirm = resolve; });
    });

    const publishing = publisher.processEvent(event.eventId);
    await publishStarted;
    const whileUnconfirmed = await OutboxResource.findOne({ resourceId }).lean();
    expect(whileUnconfirmed.outbox[0].rabbitmqStatus).toBe('pending');
    confirm();
    const delivered = await publishing;
    expect(delivered.rabbitmq).toBe('published');
  });

  it('allows only one competing publisher to claim an event', async () => {
    const { event } = await createOutboxFixture();
    let release;
    let started;
    const publishStarted = new Promise((resolve) => { started = resolve; });
    const publisherA = createTestPublisher(() => {
      started();
      return new Promise((resolve) => { release = resolve; });
    });
    const publisherB = createTestPublisher(async () => {});

    const first = publisherA.processOne(event.eventId);
    await publishStarted;
    expect(await publisherB.processOne(event.eventId)).toBeNull();
    release();
    await first;
  });

  it('recovers an expired publisher lease', async () => {
    const { resourceId, event } = await createOutboxFixture();
    await OutboxResource.updateOne({ resourceId, 'outbox.eventId': event.eventId }, {
      $set: {
        'outbox.$.claimedBy': 'crashed-publisher',
        'outbox.$.leaseUntil': new Date(Date.now() - 1000)
      }
    });
    const publisher = createTestPublisher(async () => {});

    const recovered = await publisher.processEvent(event.eventId);
    expect(recovered.rabbitmq).toBe('published');
  });

  it('cleans completed entries without deleting pending entries', async () => {
    const { resourceId, event } = await createOutboxFixture();
    const pendingEvent = createOutboxEntry('CS', 'RESOURCE_RETURNED', {
      resourceId,
      borrowRecordId: 'borrow-pending'
    });
    await OutboxResource.updateOne({ resourceId }, {
      $set: {
        'outbox.0.rabbitmqStatus': 'confirmed',
        'outbox.0.websocketRelayStatus': 'accepted_no_clients',
        'outbox.0.completedAt': new Date(Date.now() - 1000)
      }
    });
    await OutboxResource.updateOne({ resourceId }, { $push: { outbox: pendingEvent } });
    const publisher = createTestPublisher(async () => {});

    await publisher.cleanupCompleted();
    const resource = await OutboxResource.findOne({ resourceId }).lean();
    expect(resource.outbox.map((item) => item.eventId)).toEqual([pendingEvent.eventId]);
    expect(resource.outbox[0].rabbitmqStatus).toBe('pending');
  });

  it('publishes with a live RabbitMQ broker when one is available', async (context) => {
    const amqp = await import('amqplib');
    let connection;
    try {
      connection = await amqp.default.connect(liveRabbitUrl, { timeout: 500 });
    } catch {
      context.skip('RabbitMQ is unavailable; live broker delivery was not verified');
      return;
    } finally {
      if (connection) await connection.close();
    }

    const { event } = await createOutboxFixture();
    const previousUrl = process.env.RABBITMQ_URL;
    process.env.RABBITMQ_URL = liveRabbitUrl;
    const publisher = createOutboxPublisher({
      Resource: OutboxResource,
      department: 'CS',
      relayEvent: async () => 'accepted_no_clients',
      log: { info() {}, warn() {}, error() {} }
    });
    try {
      expect((await publisher.processEvent(event.eventId)).rabbitmq).toBe('published');
    } finally {
      await publisher.stop();
      if (previousUrl === undefined) delete process.env.RABBITMQ_URL;
      else process.env.RABBITMQ_URL = previousUrl;
    }
  });
});
