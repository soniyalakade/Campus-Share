import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import http from 'node:http';
import path from 'node:path';
import dotenv from 'dotenv';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import { WebSocketServer } from 'ws';
import * as grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { timingSafeEqual } from 'node:crypto';
import { createId, nowISO } from '../shared/store.js';
import { DEPARTMENTS } from '../shared/constants.js';
import { connectDatabase, disconnectDatabase, isDatabaseReady } from '../shared/database.js';
import User from './models/User.js';
import EventReceipt from './models/EventReceipt.js';

const rootEnvPath = path.resolve(process.cwd(), '.env');
dotenv.config({ path: rootEnvPath });

const app = express();
const port = Number(process.env.GATEWAY_PORT || 4000);
const jwtSecret = process.env.JWT_SECRET || 'campus-share-secret';
const authMongoUri = process.env.AUTH_MONGO_URI || 'mongodb://localhost:27017/campus_share_auth';
const internalServiceToken = process.env.INTERNAL_SERVICE_TOKEN || 'campus-share-local-internal-token';
const nodeRequestTimeoutMs = Number(process.env.NODE_REQUEST_TIMEOUT_MS) || 2500;
const nodeState = {};
const events = [];
const seenEventIds = new Set();
const serviceTargets = {
  CS: { http: process.env.CS_SERVICE_URL || 'http://localhost:5001', grpc: process.env.CS_GRPC_ADDRESS || 'localhost:6001' },
  IT: { http: process.env.IT_SERVICE_URL || 'http://localhost:5002', grpc: process.env.IT_GRPC_ADDRESS || 'localhost:6002' },
  ENTC: { http: process.env.ENTC_SERVICE_URL || 'http://localhost:5003', grpc: process.env.ENTC_GRPC_ADDRESS || 'localhost:6003' }
};

const protoPath = fileURLToPath(new URL('../protos/department.proto', import.meta.url));
const packageDefinition = protoLoader.loadSync(protoPath, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
const proto = grpc.loadPackageDefinition(packageDefinition).campus;

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws/updates' });

const addEvent = (event) => {
  const eventId = event.eventId || createId('event');
  if (seenEventIds.has(eventId)) return events.find((item) => item.eventId === eventId);
  seenEventIds.add(eventId);
  events.unshift({ ...event, eventId, id: eventId });
  if (events.length > 25) {
    const removed = events.pop();
    seenEventIds.delete(removed.eventId);
  }
  return events[0];
};

const broadcastSocket = (payload) => {
  const message = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(message);
  });
};

app.use(cors({ origin: true, credentials: true }));
app.use(helmet());
app.use(express.json());
app.use(rateLimit({ windowMs: 60 * 1000, max: 200 }));

app.use((req, _res, next) => {
  console.log(`${new Date().toISOString()} ${req.method} ${req.url}`);
  next();
});

const requireAuth = (req, res, next) => {
  const header = req.headers.authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: 'Missing bearer token' });

  try {
    const payload = jwt.verify(token, jwtSecret);
    req.user = payload;
    return next();
  } catch (error) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
};

const requireRole = (allowedRoles) => (req, res, next) => {
  const role = req.user?.role;
  if (!role || !allowedRoles.includes(role)) {
    return res.status(403).json({ error: 'Forbidden for role' });
  }
  return next();
};

const requireDepartmentAccess = (requiredDepartment) => (req, res, next) => {
  const userDepartment = req.user?.department;
  if (!userDepartment || (requiredDepartment && userDepartment !== requiredDepartment)) {
    return res.status(403).json({ error: 'Department access denied' });
  }
  return next();
};

const callDepartmentHttp = async (department, pathName, options = {}) => {
  const base = serviceTargets[department]?.http;
  if (!base) throw new Error(`Unknown department ${department}`);
  const response = await fetch(`${base}${pathName}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'X-Internal-Service-Token': internalServiceToken,
      ...(options.headers || {})
    }
  });
  if (!response.ok) {
    const errorPayload = await response.json().catch(() => ({}));
    const error = new Error(errorPayload.error || `Request failed with ${response.status}`);
    error.status = response.status;
    throw error;
  }
  return response.json().catch(() => null);
};

const callDepartmentAsActor = (department, pathName, actor, options = {}) => callDepartmentHttp(department, pathName, {
  ...options,
  headers: {
    ...(options.headers || {}),
    'X-Actor-Id': actor.userId,
    'X-Actor-Role': actor.role,
    'X-Actor-Department': actor.department || ''
  }
});

const respondWithDepartmentError = (res, error) => {
  const upstreamStatus = Number.isInteger(error?.status) ? error.status : 503;
  const status = upstreamStatus === 401 ? 503 : upstreamStatus;
  const message = upstreamStatus === 401
    ? 'Department service rejected Gateway credentials'
    : error.message || 'Department service unavailable';
  return res.status(status).json({ error: message });
};

const searchDepartmentResource = async (department, name, timeoutMs = 2000) => {
  try {
    const client = new proto.DepartmentService(serviceTargets[department].grpc, grpc.credentials.createInsecure());
    const response = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${department} search timed out`)), timeoutMs);
      client.SearchResource({ name }, (error, result) => {
        clearTimeout(timer);
        if (error) return reject(error);
        return resolve(result?.resources || []);
      });
    });
    return response;
  } catch (_error) {
    const resources = await callDepartmentHttp(department, '/internal/resources').catch(() => []);
    const query = String(name || '').trim().toLowerCase();
    return (resources || []).filter((item) => {
      const haystack = `${item.name || ''} ${item.category || ''} ${item.description || ''}`.toLowerCase();
      return haystack.includes(query);
    });
  }
};

const fetchNodeState = async (department) => {
  const [healthResult, leaderResult] = await Promise.allSettled([
    callDepartmentHttp(department, '/internal/health', { signal: AbortSignal.timeout(nodeRequestTimeoutMs) }),
    callDepartmentHttp(department, '/internal/leader', { signal: AbortSignal.timeout(nodeRequestTimeoutMs) })
  ]);
  const errorFor = (result) => (result.status === 'rejected' ? result.reason : null);
  const health = healthResult.status === 'fulfilled' ? healthResult.value : null;
  const leader = leaderResult.status === 'fulfilled' ? leaderResult.value : null;
  const healthError = errorFor(healthResult);
  let status = 'unknown';
  if (health?.status === 'healthy') {
    status = 'online';
  } else if (healthError) {
    if (healthError.name !== 'TimeoutError' && healthError.name !== 'AbortError'
      && (healthError.status >= 500 || healthError.cause?.code === 'ECONNREFUSED')) {
      status = 'offline';
    }
  } else if (['database_unavailable', 'offline', 'unhealthy'].includes(health?.status)) {
    status = 'offline';
  }
  return {
    department,
    nodeId: health?.nodeId || department,
    priority: leader?.priority ?? health?.priority ?? null,
    status,
    serviceStatus: health?.status || 'unknown',
    availability: status,
    leader: leader?.coordinator || health?.leader || null,
    leaderPriority: leader?.leaderPriority ?? null,
    electionStatus: leader?.electionStatus || health?.electionStatus || 'unknown',
    latestResult: leader?.latestResult || null,
    lamportClock: Number.isSafeInteger(health?.lamportClock) ? health.lamportClock : null,
    lastHeartbeat: nodeState[department]?.lastHeartbeat || health?.lastHeartbeat || null,
    lastHealthCheck: health ? nowISO() : null,
    healthCheck: healthError ? 'failed' : 'ok',
    healthError: healthError?.message || null,
    leaderError: errorFor(leaderResult)?.message || null
  };
};

const findResourceLocation = async (resourceId) => {
  const searches = await Promise.allSettled(DEPARTMENTS.map(async (department) => {
    const resource = await callDepartmentHttp(department, `/internal/resources/${encodeURIComponent(resourceId)}`);
    return { department, resource };
  }));
  const location = searches.find((result) => result.status === 'fulfilled');
  if (location) return location.value;
  const serviceError = searches.find((result) => result.status === 'rejected' && result.reason.status !== 404);
  if (serviceError) throw serviceError.reason;
  return null;
};

const findReservationLocation = async (reservationId, actor) => {
  const departments = actor.role === 'DEPARTMENT_ADMIN' ? [actor.department] : DEPARTMENTS;
  const results = await Promise.allSettled(departments.map(async (department) => {
    const reservation = await callDepartmentAsActor(
      department,
      `/internal/reservations/${encodeURIComponent(reservationId)}`,
      actor
    );
    return { department, reservation };
  }));
  const location = results.find((result) => result.status === 'fulfilled');
  if (location) {
    return {
      location: location.value,
      denied: results.some((result) => result.status === 'rejected' && result.reason.status === 403)
    };
  }
  const serviceError = results.find((result) => (
    result.status === 'rejected' && ![403, 404].includes(result.reason.status)
  ));
  if (serviceError) throw serviceError.reason;
  return {
    location: null,
    denied: results.some((result) => result.status === 'rejected' && result.reason.status === 403)
  };
};

app.get('/health', (_req, res) => {
  if (!isDatabaseReady()) {
    return res.status(503).json({ status: 'database_unavailable', service: 'gateway', timestamp: nowISO() });
  }
  res.json({ status: 'ok', service: 'gateway', timestamp: nowISO() });
});

app.post('/api/auth/register', async (req, res) => {
  const { name, email, password, role, department, adminRegistrationKey } = req.body || {};

  if (!name || !email || !password) {
    return res.status(400).json({ error: 'Name, email, and password are required' });
  }
  if (!['STUDENT', 'DEPARTMENT_ADMIN'].includes(role)) {
    return res.status(400).json({ error: 'Invalid role' });
  }
  if (role === 'DEPARTMENT_ADMIN' && !DEPARTMENTS.includes(department)) {
    return res.status(400).json({ error: 'Invalid department assignment for admin' });
  }
  if (role === 'DEPARTMENT_ADMIN') {
    const expectedKey = process.env.ADMIN_REGISTRATION_KEY;
    const supplied = typeof adminRegistrationKey === 'string' ? Buffer.from(adminRegistrationKey) : Buffer.alloc(0);
    const expected = typeof expectedKey === 'string' ? Buffer.from(expectedKey) : Buffer.alloc(0);
    const validKey = expected.length > 0
      && supplied.length === expected.length
      && timingSafeEqual(supplied, expected);
    if (!validKey) {
      return res.status(403).json({ error: 'Department administrator registration is disabled or the registration key is invalid' });
    }
  }
  try {
    const passwordHash = await bcrypt.hash(String(password), 10);
    const user = await User.create({
      userId: createId('user'),
      name: String(name),
      email: String(email).toLowerCase(),
      passwordHash,
      role,
      department: role === 'DEPARTMENT_ADMIN' ? department : null
    });
    const token = jwt.sign({ userId: user.userId, email: user.email, role: user.role, department: user.department }, jwtSecret, { expiresIn: '7d' });
    addEvent({ eventType: 'USER_REGISTERED', sourceNode: 'gateway', payload: { userId: user.userId, email: user.email } });
    return res.status(201).json({ token, user: {
      userId: user.userId,
      name: user.name,
      email: user.email,
      role: user.role,
      department: user.department,
      createdAt: user.createdAt,
      updatedAt: user.updatedAt
    } });
  } catch (error) {
    if (error?.code === 11000) return res.status(409).json({ error: 'Email already registered' });
    console.error(`User registration failed: ${error.message}`);
    return res.status(503).json({ error: 'Authentication database is unavailable' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const { email, password } = req.body || {};
  try {
    const user = await User.findOne({ email: String(email).toLowerCase() }).select('+passwordHash');
    if (!user) return res.status(401).json({ error: 'Invalid credentials' });
    const match = await bcrypt.compare(String(password), user.passwordHash);
    if (!match) return res.status(401).json({ error: 'Invalid credentials' });

    const token = jwt.sign({ userId: user.userId, email: user.email, role: user.role, department: user.department }, jwtSecret, { expiresIn: '7d' });
    addEvent({ eventType: 'USER_LOGIN', sourceNode: 'gateway', payload: { userId: user.userId } });
    return res.json({ token, user: {
      userId: user.userId,
      name: user.name,
      email: user.email,
      role: user.role,
      department: user.department,
      createdAt: user.createdAt,
      updatedAt: user.updatedAt
    } });
  } catch (error) {
    console.error(`User login failed: ${error.message}`);
    return res.status(503).json({ error: 'Authentication database is unavailable' });
  }
});

app.get('/api/resources/search', async (req, res) => {
  const query = typeof req.query.name === 'string' ? req.query.name.trim() : '';
  if (!query) {
    return res.status(400).json({ error: 'Search query parameter name is required' });
  }

  const resultMap = { CS: [], IT: [], ENTC: [] };
  const nodesUnreachable = [];

  const settled = await Promise.allSettled(DEPARTMENTS.map(async (department) => {
    const items = await searchDepartmentResource(department, query);
    resultMap[department] = items;
  }));

  settled.forEach((item, index) => {
    if (item.status === 'rejected') {
      const department = DEPARTMENTS[index];
      nodesUnreachable.push(department);
    }
  });

  const total = Object.values(resultMap).reduce((sum, list) => sum + list.length, 0);
  return res.json({ query, total, departments: resultMap, nodesUnreachable });
});

app.get('/api/resources', async (_req, res) => {
  const settled = await Promise.allSettled(DEPARTMENTS.map(async (department) => {
    const resources = await callDepartmentHttp(department, '/internal/resources');
    if (!Array.isArray(resources)) throw new Error(`Invalid resource catalogue response from ${department}`);
    return resources;
  }));
  const resources = [];
  const nodesUnreachable = [];

  settled.forEach((result, index) => {
    if (result.status === 'rejected') {
      nodesUnreachable.push(DEPARTMENTS[index]);
      return;
    }
    for (const resource of result.value) {
      if (!resource?.resourceId) continue;
      resources.push({
        resourceId: resource.resourceId,
        name: resource.name,
        category: resource.category,
        description: resource.description,
        department: resource.department,
        laboratory: resource.laboratory,
        totalQuantity: resource.totalQuantity,
        availableQuantity: resource.availableQuantity,
        reservedQuantity: resource.reservedQuantity,
        borrowedQuantity: resource.borrowedQuantity,
        condition: resource.condition,
        imageUrl: resource.imageUrl,
        manualUrl: resource.manualUrl,
        tutorialVideoUrl: resource.tutorialVideoUrl
      });
    }
  });

  if (nodesUnreachable.length === DEPARTMENTS.length) {
    return res.status(503).json({
      error: `Resource catalogue unavailable; department services could not be reached: ${nodesUnreachable.join(', ')}.`,
      nodesUnreachable
    });
  }
  return res.json({ resources, nodesUnreachable });
});

app.get('/api/resources/:id', async (req, res) => {
  try {
    const location = await findResourceLocation(req.params.id);
    if (!location) return res.status(404).json({ error: 'Resource not found' });
    return res.json(location.resource);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
});

const listActorRecords = async (kind, actor) => {
  const departments = actor.role === 'DEPARTMENT_ADMIN' ? [actor.department] : DEPARTMENTS;
  const settled = await Promise.allSettled(departments.map((department) => {
    const query = actor.role === 'STUDENT' ? `?studentId=${encodeURIComponent(actor.userId)}` : '';
    return callDepartmentAsActor(department, `/internal/${kind}${query}`, actor);
  }));
  const records = [];
  const nodesUnreachable = [];
  settled.forEach((result, index) => {
    if (result.status === 'fulfilled') records.push(...result.value);
    else if (result.reason.status !== 403 && result.reason.status !== 404) nodesUnreachable.push(departments[index]);
  });
  return { records, nodesUnreachable };
};

app.post('/api/reservations', requireAuth, requireRole(['STUDENT']), async (req, res) => {
  const resourceId = typeof req.body?.resourceId === 'string' ? req.body.resourceId.trim() : '';
  if (!resourceId) return res.status(400).json({ error: 'resourceId is required' });
  try {
    const location = await findResourceLocation(resourceId);
    if (!location) return res.status(404).json({ error: 'Resource not found' });
    const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
    const result = await callDepartmentAsActor(
      location.department,
      `/internal/resources/${encodeURIComponent(resourceId)}/reservations`,
      actor,
      { method: 'POST', body: JSON.stringify({}) }
    );
    return res.status(201).json(result);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
});

const getReservations = async (req, res) => {
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  const result = await listActorRecords('reservations', actor);
  return res.json({ reservations: result.records, nodesUnreachable: result.nodesUnreachable });
};

app.get('/api/reservations', requireAuth, getReservations);
app.get('/api/reservations/my', requireAuth, requireRole(['STUDENT']), getReservations);

app.get('/api/reservations/:id', requireAuth, async (req, res) => {
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  try {
    const result = await findReservationLocation(req.params.id, actor);
    if (result.location) return res.json(result.location.reservation);
    if (result.denied) return res.status(403).json({ error: 'You may only view your own reservation or your department reservations' });
    return res.status(404).json({ error: 'Reservation not found' });
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
});

app.delete('/api/reservations/:id', requireAuth, requireRole(['STUDENT', 'DEPARTMENT_ADMIN']), async (req, res) => {
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  try {
    const result = await findReservationLocation(req.params.id, actor);
    if (result.denied && !result.location) return res.status(403).json({ error: 'You may only cancel your own reservation or a reservation in your department' });
    if (!result.location) return res.status(404).json({ error: 'Reservation not found' });
    const operation = await callDepartmentAsActor(
      result.location.department,
      `/internal/reservations/${encodeURIComponent(req.params.id)}/cancel`,
      actor,
      { method: 'POST', body: JSON.stringify({}) }
    );
    return res.json(operation);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
});

app.patch('/api/reservations/:id/status', requireAuth, requireRole(['DEPARTMENT_ADMIN']), async (req, res) => {
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  const requested = req.body?.status;
  if (!['APPROVED', 'REJECTED'].includes(requested)) {
    return res.status(400).json({ error: 'status must be APPROVED or REJECTED' });
  }
  try {
    const result = await callDepartmentAsActor(
      actor.department,
      `/internal/reservations/${encodeURIComponent(req.params.id)}/status`,
      actor,
      { method: 'PATCH', body: JSON.stringify({ status: requested }) }
    );
    return res.json(result);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
});

app.get('/api/borrow-records/my', requireAuth, async (req, res) => {
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  const result = await listActorRecords('borrow-records', actor);
  return res.json({ borrowRecords: result.records, nodesUnreachable: result.nodesUnreachable });
});

app.post('/api/resources', requireAuth, requireRole(['DEPARTMENT_ADMIN']), requireDepartmentAccess(null), async (req, res) => {
  const department = req.user.department;
  const actor = { userId: req.user.userId, role: req.user.role, department };
  try {
    const resource = await callDepartmentAsActor(department, '/internal/resources', actor, {
      method: 'POST',
      body: JSON.stringify(req.body || {})
    });
    return res.status(201).json(resource);
  } catch (error) {
    return res.status(400).json({ error: error.message });
  }
});

app.put('/api/resources/:id', requireAuth, requireRole(['DEPARTMENT_ADMIN']), async (req, res) => {
  let location;
  try {
    location = await findResourceLocation(req.params.id);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
  if (!location) return res.status(404).json({ error: 'Resource not found' });
  if (location.department !== req.user.department) {
    return res.status(403).json({ error: 'Department ownership mismatch' });
  }
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  try {
    const resource = await callDepartmentAsActor(location.department, `/internal/resources/${req.params.id}`, actor, {
      method: 'PUT',
      body: JSON.stringify(req.body || {})
    });
    return res.json(resource);
  } catch (error) {
    return res.status(400).json({ error: error.message });
  }
});

app.delete('/api/resources/:id', requireAuth, requireRole(['DEPARTMENT_ADMIN']), async (req, res) => {
  let location;
  try {
    location = await findResourceLocation(req.params.id);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
  if (!location) return res.status(404).json({ error: 'Resource not found' });
  if (location.department !== req.user.department) {
    return res.status(403).json({ error: 'Department ownership mismatch' });
  }
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  try {
    const result = await callDepartmentAsActor(location.department, `/internal/resources/${req.params.id}`, actor, {
      method: 'DELETE'
    });
    return res.json(result);
  } catch (error) {
    return res.status(400).json({ error: error.message });
  }
});

app.post('/api/resources/:id/borrow', requireAuth, async (req, res) => {
  let location;
  try {
    location = await findResourceLocation(req.params.id);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
  if (!location) return res.status(404).json({ error: 'Resource not found' });
  const reservationId = typeof req.body?.reservationId === 'string' ? req.body.reservationId.trim() : '';
  if (!reservationId) return res.status(400).json({ error: 'reservationId is required for borrowing' });
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  try {
    const result = await callDepartmentAsActor(location.department, `/internal/resources/${encodeURIComponent(req.params.id)}/borrow`, actor, {
      method: 'POST',
      body: JSON.stringify({ reservationId })
    });
    return res.status(201).json(result);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
});

app.post('/api/resources/:id/return', requireAuth, async (req, res) => {
  let location;
  try {
    location = await findResourceLocation(req.params.id);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
  if (!location) return res.status(404).json({ error: 'Resource not found' });
  const borrowRecordId = typeof req.body?.borrowRecordId === 'string' ? req.body.borrowRecordId.trim() : '';
  if (!borrowRecordId) return res.status(400).json({ error: 'borrowRecordId is required to return a borrowed resource' });
  const actor = { userId: req.user.userId, role: req.user.role, department: req.user.department };
  try {
    const result = await callDepartmentAsActor(location.department, `/internal/resources/${encodeURIComponent(req.params.id)}/return`, actor, {
      method: 'POST',
      body: JSON.stringify({ borrowRecordId })
    });
    return res.json(result);
  } catch (error) {
    return respondWithDepartmentError(res, error);
  }
});

app.post('/internal/events/notify', (req, res) => {
  if (req.get('X-Internal-Service-Token') !== internalServiceToken) {
    return res.status(401).json({ error: 'Invalid internal service credentials' });
  }
  const event = req.body || {};
  if (typeof event.eventId !== 'string' || !event.eventId || typeof event.resourceId !== 'string'
    || typeof event.eventType !== 'string' || typeof event.sourceDepartment !== 'string') {
    return res.status(400).json({ error: 'eventId, eventType, sourceDepartment, and resourceId are required' });
  }
  const relayEvent = async () => {
    const normalized = {
      ...event,
      sourceNode: event.sourceNode || event.sourceDepartment,
      timestamp: event.timestamp || nowISO(),
      payload: event.payload || {}
    };
    try {
      await EventReceipt.updateOne(
        { eventId: event.eventId },
        { $setOnInsert: { eventId: event.eventId, relayStatus: 'pending', event: normalized } },
        { upsert: true }
      );
      const now = new Date();
      const receipt = await EventReceipt.findOneAndUpdate({
        eventId: event.eventId,
        relayStatus: 'pending',
        $or: [{ relayLeaseUntil: null }, { relayLeaseUntil: { $lte: now } }]
      }, {
        $set: { relayLeaseUntil: new Date(now.getTime() + 10000), updatedAt: now }
      }, { new: true }).lean();
      if (!receipt) {
        const existing = await EventReceipt.findOne({ eventId: event.eventId }).lean();
        const status = existing?.relayStatus || 'pending';
        return res.json({
          ok: true,
          duplicate: true,
          connectedClients: [...wss.clients].filter((client) => client.readyState === 1).length,
          relayed: status === 'relayed',
          websocketRelayStatus: status
        });
      }
      const connectedClients = [...wss.clients].filter((client) => client.readyState === 1).length;
      const relayStatus = connectedClients > 0 ? 'relayed' : 'accepted_no_clients';
      const alreadyBuffered = seenEventIds.has(event.eventId);
      const accepted = addEvent({
        ...receipt.event,
        messagingStatus: { ...(receipt.event.messagingStatus || {}), websocketRelay: relayStatus }
      });
      if (connectedClients > 0) {
        if (accepted && !alreadyBuffered) broadcastSocket({ type: 'EVENT', event: accepted });
      }
      await EventReceipt.updateOne({ eventId: event.eventId, relayStatus: 'pending' }, {
        $set: {
          relayStatus,
          relayLeaseUntil: null,
          expireAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
          updatedAt: new Date()
        }
      });
      return res.json({
        ok: true,
        duplicate: false,
        connectedClients,
        relayed: relayStatus === 'relayed',
        websocketRelayStatus: relayStatus
      });
    } catch (error) {
      console.error(`Gateway event receipt failed (${error.code || error.name})`);
      return res.status(503).json({ error: 'Gateway event relay temporarily unavailable' });
    }
  };
  return relayEvent();
});

app.post('/internal/heartbeat', (req, res) => {
  if (req.get('X-Internal-Service-Token') !== internalServiceToken) {
    return res.status(401).json({ error: 'Invalid internal service credentials' });
  }
  const body = req.body || {};
  const department = body.department;
  if (!DEPARTMENTS.includes(department) || body.status !== 'healthy') {
    return res.status(400).json({ error: 'A valid department and healthy status are required' });
  }
  nodeState[department] = {
    department,
    status: body.status,
    lastHeartbeat: nowISO(),
    nodeId: body.nodeId || department,
    health: 'ok'
  };
  res.json({ ok: true, department, lastHeartbeat: nodeState[department].lastHeartbeat });
});

app.get('/api/nodes', async (_req, res) => {
  const states = await Promise.allSettled(DEPARTMENTS.map((department) => fetchNodeState(department)));
  const payload = states.map((result, index) => (result.status === 'fulfilled'
    ? result.value
    : { department: DEPARTMENTS[index], status: 'unknown', availability: 'unknown', error: result.reason.message }));
  return res.json(payload);
});

app.get('/api/nodes/health', async (_req, res) => {
  const payload = await Promise.allSettled(DEPARTMENTS.map((department) => fetchNodeState(department)));
  return res.json({ nodes: payload.map((item, index) => (item.status === 'fulfilled'
    ? item.value
    : { department: DEPARTMENTS[index], status: 'unknown', availability: 'unknown', error: item.reason.message })) });
});

app.get('/api/distributed/leader', async (_req, res) => {
  const states = await Promise.allSettled(DEPARTMENTS.map((department) => callDepartmentHttp(department, '/internal/leader')));
  return res.json({ leaders: states.map((result, index) => (result.status === 'fulfilled'
    ? result.value
    : { department: DEPARTMENTS[index], status: 'unknown', coordinator: null, error: result.reason.message })) });
});

app.get('/api/distributed/lamport', async (_req, res) => {
  const states = await Promise.allSettled(DEPARTMENTS.map((department) => callDepartmentHttp(department, '/internal/distributed/lamport')));
  return res.json({ lamport: states.map((result, index) => (result.status === 'fulfilled'
    ? result.value
    : { department: DEPARTMENTS[index], status: 'unknown', lamportClock: null, error: result.reason.message })) });
});

app.get('/api/distributed/vector', async (_req, res) => {
  const states = await Promise.allSettled(DEPARTMENTS.map((department) => callDepartmentHttp(department, '/internal/distributed/vector')));
  return res.json({ vector: states.map((result) => (result.status === 'fulfilled' ? result.value : { department: 'unknown', vectorClock: { CS: 0, IT: 0, ENTC: 0 } })) });
});

app.post('/api/distributed/election', async (_req, res) => {
  const errors = [];
  for (const department of ['ENTC', 'IT', 'CS']) {
    try {
      const result = await callDepartmentHttp(department, '/internal/election', {
        method: 'POST',
        body: '{}',
        signal: AbortSignal.timeout(Math.max(nodeRequestTimeoutMs * 4, 12000))
      });
      if (!result?.coordinator) {
        return res.status(503).json({ error: 'The department election completed without an available leader', triggerNode: department, result });
      }
      return res.json({ triggerNode: department, result });
    } catch (error) {
      errors.push({ department, error: error.message });
    }
  }
  return res.status(503).json({ error: 'No department service could accept the election trigger', errors });
});

wss.on('connection', (socket) => {
  socket.send(JSON.stringify({ type: 'CONNECTED', message: 'WebSocket connected to Campus Share gateway', events }));
  socket.on('message', (message) => {
    const data = message.toString();
    if (data === 'ping') socket.send(JSON.stringify({ type: 'PONG', at: nowISO() }));
  });
});

const isDirectEntry = (() => {
  try {
    return pathToFileURL(process.argv[1] || '').href === pathToFileURL(fileURLToPath(import.meta.url)).href;
  } catch (_error) {
    return false;
  }
})();

export async function initializeGateway(uri = authMongoUri) {
  await connectDatabase(uri, 'Gateway authentication');
  await User.init();
  await EventReceipt.init();
}

if (isDirectEntry) {
  initializeGateway().then(() => {
    server.listen(port, () => {
      console.log(`Gateway listening on http://localhost:${port}`);
      console.log(`WebSocket relay active on ws://localhost:${port}/ws/updates`);
    });
  }).catch((error) => {
    console.error(`Gateway startup failed: ${error.message}`);
    process.exitCode = 1;
  });
  const shutdown = async () => {
    server.close();
    await disconnectDatabase();
  };
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}

export { app, addEvent, broadcastSocket, events, server, wss };
