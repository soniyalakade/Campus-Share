import { afterEach, describe, expect, it, vi } from 'vitest';
import request from 'supertest';
import WebSocket from 'ws';
import { app, server } from '../gateway/server.js';
import EventReceipt from '../gateway/models/EventReceipt.js';
import { BullyElection } from '../shared/bullyElection.js';
import { LamportClock } from '../shared/lamportClock.js';
import { createOutboxEntry, createOutboxPublisher } from '../shared/outboxPublisher.js';
import { ResourceSchema } from '../services/models/Resource.js';

function createCluster(initiallyAvailable = ['CS', 'IT', 'ENTC']) {
  const available = new Set(initiallyAvailable);
  const nodes = new Map();
  for (const department of ['CS', 'IT', 'ENTC']) {
    nodes.set(department, new BullyElection({
      department,
      clock: new LamportClock(),
      timeoutMs: 10,
      coordinatorWaitMs: 150,
      retryDelayMs: 2,
      sendMessage: async (peer, _path, message) => {
        if (!available.has(peer)) throw new Error(`${peer} unavailable`);
        return nodes.get(peer).receiveMessage(message);
      },
      checkPeer: async (peer) => available.has(peer)
    }));
  }
  return { available, nodes };
}

describe('Lamport logical clocks', () => {
  it('ticks local events and receives using max(local, remote) + 1', () => {
    const clock = new LamportClock();
    expect(clock.tick()).toBe(1);
    expect(clock.receive(8)).toBe(9);
    expect(clock.receive(3)).toBe(10);
    expect(clock.getTimestamp()).toBe(10);
  });

  it('rejects invalid timestamps without changing the clock', () => {
    const clock = new LamportClock(4);
    for (const value of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1, '5', NaN]) {
      expect(() => clock.receive(value)).toThrow();
    }
    expect(clock.getTimestamp()).toBe(4);
  });

  it('maintains independent department clock instances', () => {
    const cs = new LamportClock();
    const it = new LamportClock();
    cs.tick();
    cs.tick();
    expect(cs.getTimestamp()).toBe(2);
    expect(it.getTimestamp()).toBe(0);
  });
});

describe('Lamport lifecycle outbox metadata', () => {
  it('persists the schema field and carries it through RabbitMQ and Gateway relay payloads', async () => {
    expect(ResourceSchema.path('outbox').schema.path('lamportTimestamp').instance).toBe('Number');
    const entry = {
      ...createOutboxEntry('CS', 'RESOURCE_RESERVED', { resourceId: 'isolated-resource' }),
      lamportTimestamp: 42
    };
    const storedResource = {
      resourceId: 'isolated-resource',
      isDeleted: false,
      outbox: [{ ...entry }]
    };
    const Resource = {
      findOneAndUpdate: (_query, update) => ({
        lean: async () => {
          if (update.$inc?.['outbox.$.attempts']) storedResource.outbox[0].attempts += 1;
          if (update.$set?.['outbox.$.claimedBy']) {
            storedResource.outbox[0].claimedBy = update.$set['outbox.$.claimedBy'];
          }
          if (update.$set?.['outbox.$.leaseUntil']) {
            storedResource.outbox[0].leaseUntil = update.$set['outbox.$.leaseUntil'];
          }
          return structuredClone(storedResource);
        }
      }),
      updateOne: async (_query, update) => {
        for (const [path, value] of Object.entries(update.$set || {})) {
          storedResource.outbox[0][path.slice('outbox.$.'.length)] = value;
        }
        for (const path of Object.keys(update.$unset || {})) {
          delete storedResource.outbox[0][path.slice('outbox.$.'.length)];
        }
        return { matchedCount: 1 };
      },
      findOne: () => ({
        select() {
          return this;
        },
        lean: async () => structuredClone({
          resourceId: storedResource.resourceId,
          outbox: [storedResource.outbox[0]]
        })
      }),
      aggregate: async () => []
    };
    let rabbitEvent;
    let relayedEvent;
    const publisher = createOutboxPublisher({
      Resource,
      department: 'CS',
      publishRabbit: async (event) => { rabbitEvent = event; },
      relayEvent: async (event) => {
        relayedEvent = event;
        return 'relayed';
      },
      log: { info() {}, warn() {}, error() {} }
    });

    const delivery = await publisher.processEvent(entry.eventId);
    await publisher.stop();

    expect(storedResource.outbox[0].lamportTimestamp).toBe(42);
    expect(rabbitEvent.lamportTimestamp).toBe(42);
    expect(relayedEvent.lamportTimestamp).toBe(42);
    expect(delivery).toMatchObject({ rabbitmq: 'published', websocketRelay: 'relayed' });
  });

  it('relays an outbox lifecycle event with its Lamport timestamp to a connected WebSocket client', async () => {
    const entry = {
      ...createOutboxEntry('CS', 'RESOURCE_UPDATED', { resourceId: 'isolated-websocket-resource' }),
      lamportTimestamp: 84
    };
    const storedResource = {
      resourceId: entry.resourceId,
      isDeleted: false,
      outbox: [{ ...entry }]
    };
    const Resource = {
      findOneAndUpdate: (_query, update) => ({
        lean: async () => {
          storedResource.outbox[0].attempts += update.$inc['outbox.$.attempts'];
          Object.assign(storedResource.outbox[0], {
            claimedBy: update.$set['outbox.$.claimedBy'],
            leaseUntil: update.$set['outbox.$.leaseUntil']
          });
          return structuredClone(storedResource);
        }
      }),
      updateOne: async (_query, update) => {
        for (const [path, value] of Object.entries(update.$set || {})) {
          storedResource.outbox[0][path.slice('outbox.$.'.length)] = value;
        }
        for (const path of Object.keys(update.$unset || {})) {
          delete storedResource.outbox[0][path.slice('outbox.$.'.length)];
        }
        return { matchedCount: 1 };
      },
      findOne: () => ({
        select() {
          return this;
        },
        lean: async () => structuredClone({
          resourceId: storedResource.resourceId,
          outbox: [storedResource.outbox[0]]
        })
      }),
      aggregate: async () => []
    };
    const receipts = new Map();
    const updateReceipt = vi.spyOn(EventReceipt, 'updateOne').mockImplementation(async (filter, update) => {
      if (!receipts.has(filter.eventId) && update.$setOnInsert) {
        receipts.set(filter.eventId, {
          eventId: filter.eventId,
          relayStatus: update.$setOnInsert.relayStatus,
          event: update.$setOnInsert.event
        });
      } else if (update.$set) {
        Object.assign(receipts.get(filter.eventId), update.$set);
      }
      return { acknowledged: true };
    });
    const claimReceipt = vi.spyOn(EventReceipt, 'findOneAndUpdate').mockImplementation((filter, update) => ({
      lean: async () => {
        const receipt = receipts.get(filter.eventId);
        if (!receipt || receipt.relayStatus !== 'pending') return null;
        Object.assign(receipt, update.$set);
        return structuredClone(receipt);
      }
    }));

    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, resolve);
    });
    const address = server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/ws/updates`);
    let publisher;
    try {
      await new Promise((resolve, reject) => {
        socket.once('open', resolve);
        socket.once('error', reject);
      });
      const received = new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Timed out waiting for the relayed lifecycle event')), 3000);
        socket.on('message', (raw) => {
          const message = JSON.parse(raw.toString());
          if (message.type === 'EVENT' && message.event.eventId === entry.eventId) {
            clearTimeout(timer);
            resolve(message.event);
          }
        });
      });
      let rabbitEvent;
      publisher = createOutboxPublisher({
        Resource,
        department: 'CS',
        publishRabbit: async (event) => { rabbitEvent = event; },
        relayEvent: async (event) => {
          const response = await fetch(`http://127.0.0.1:${address.port}/internal/events/notify`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Internal-Service-Token': process.env.INTERNAL_SERVICE_TOKEN || 'campus-share-local-internal-token'
            },
            body: JSON.stringify(event)
          });
          if (!response.ok) throw new Error(`Gateway relay failed with HTTP ${response.status}`);
          const result = await response.json();
          return result.websocketRelayStatus;
        },
        retryBaseMs: 0,
        retryMaxMs: 0,
        log: { info() {}, warn() {}, error() {} }
      });

      const delivery = await publisher.processEvent(entry.eventId);
      const websocketEvent = await received;

      expect(rabbitEvent).toMatchObject({
        eventId: entry.eventId,
        eventType: 'RESOURCE_UPDATED',
        lamportTimestamp: 84
      });
      expect(receipts.get(entry.eventId)).toMatchObject({
        relayStatus: 'relayed',
        event: {
          eventType: 'RESOURCE_UPDATED',
          sourceDepartment: 'CS',
          resourceId: entry.resourceId,
          lamportTimestamp: 84
        }
      });
      expect(websocketEvent).toMatchObject({
        eventId: entry.eventId,
        eventType: 'RESOURCE_UPDATED',
        sourceDepartment: 'CS',
        resourceId: entry.resourceId,
        lamportTimestamp: 84
      });
      expect(delivery).toMatchObject({ rabbitmq: 'published', websocketRelay: 'relayed' });
      expect(storedResource.outbox[0]).toMatchObject({
        rabbitmqStatus: 'confirmed',
        websocketRelayStatus: 'relayed',
        lamportTimestamp: 84
      });
    } finally {
      if (publisher) await publisher.stop();
      socket.terminate();
      await new Promise((resolve) => server.close(resolve));
      updateReceipt.mockRestore();
      claimReceipt.mockRestore();
    }
  });
});

describe('Bully leader election', () => {
  it('elects ENTC when all department nodes are available', async () => {
    const { nodes } = createCluster();
    const election = nodes.get('CS').startElection();
    expect(nodes.get('CS').startElection()).toBe(election);
    await election;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(nodes.get('CS').getState().coordinator).toBe('ENTC');
    expect(nodes.get('IT').getState().coordinator).toBe('ENTC');
    expect(nodes.get('ENTC').getState().coordinator).toBe('ENTC');
  });

  it('elects IT when ENTC is unavailable', async () => {
    const { nodes } = createCluster(['CS', 'IT']);
    await nodes.get('CS').startElection();
    expect(nodes.get('CS').getState().coordinator).toBe('IT');
    expect(nodes.get('IT').getState().coordinator).toBe('IT');
  });

  it('elects CS when IT and ENTC are unavailable', async () => {
    const { nodes } = createCluster(['CS']);
    await nodes.get('CS').startElection();
    expect(nodes.get('CS').getState().coordinator).toBe('CS');
  });

  it('re-elects a recovered higher-priority node', async () => {
    const { available, nodes } = createCluster(['CS', 'IT']);
    await nodes.get('IT').startElection();
    expect(nodes.get('IT').getState().coordinator).toBe('IT');
    available.add('ENTC');
    await nodes.get('IT').checkPeers();
    expect(nodes.get('IT').getState().coordinator).toBe('ENTC');
    expect(nodes.get('ENTC').getState().coordinator).toBe('ENTC');
  });

  it('elects a successor after leader unavailability and re-elects the recovered higher-priority node', async () => {
    const { available, nodes } = createCluster();
    await nodes.get('CS').startElection();
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(nodes.get('IT').getState().coordinator).toBe('ENTC');

    available.delete('ENTC');
    const successor = await nodes.get('IT').checkPeers();
    expect(successor.coordinator).toBe('IT');
    expect(successor.latestResult.reason).toBe('leader-unreachable');
    expect(nodes.get('CS').getState().coordinator).toBe('IT');

    available.add('ENTC');
    const recovered = await nodes.get('IT').checkPeers();
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(recovered.coordinator).toBe('ENTC');
    expect(recovered.latestResult.reason).toBe('higher-priority-node-recovered');
    expect(nodes.get('CS').getState().coordinator).toBe('ENTC');
    expect(nodes.get('ENTC').getState().coordinator).toBe('ENTC');
  });

  it('settles a pending lower-priority election as coordinator_announced after a higher leader arrives', async () => {
    const cs = new BullyElection({
      department: 'CS',
      clock: new LamportClock(),
      timeoutMs: 10,
      coordinatorWaitMs: 100,
      retryDelayMs: 2,
      checkPeer: async () => true,
      sendMessage: async (_peer, _path, message) => {
        if (message.type === 'ELECTION') {
          setTimeout(() => {
            cs.receiveMessage({
              type: 'COORDINATOR',
              sender: 'ENTC',
              messageId: `${message.electionId}:coordinator:ENTC`,
              electionId: message.electionId,
              lamportTimestamp: message.lamportTimestamp + 1
            });
          }, 0);
          return { response: 'OK', lamportTimestamp: message.lamportTimestamp + 1 };
        }
        return { accepted: true, lamportTimestamp: message.lamportTimestamp + 1 };
      }
    });

    const csElection = await cs.startElection();
    expect(csElection.coordinator).toBe('ENTC');
    expect(csElection.electionStatus).toBe('coordinator_announced');
    expect(csElection.latestResult).toMatchObject({ success: true, coordinator: 'ENTC' });
    expect(cs.getState().electionStatus).toBe('coordinator_announced');
  });

  it('acknowledges duplicate election messages without receiving their clock twice', async () => {
    const receiverClock = new LamportClock();
    const receiver = new BullyElection({
      department: 'IT',
      clock: receiverClock,
      sendMessage: async () => ({}),
      checkPeer: async () => false
    });
    const message = {
      type: 'ELECTION',
      sender: 'CS',
      messageId: 'duplicate-test',
      electionId: 'test-election',
      lamportTimestamp: 5
    };
    const first = await receiver.receiveMessage(message);
    const duplicate = await receiver.receiveMessage(message);
    expect(first.response).toBe('OK');
    expect(duplicate.response).toBe('OK');
    expect(duplicate.duplicate).toBe(true);
    expect(receiverClock.getTimestamp()).toBe(8);
    await expect(receiver.receiveMessage({
      ...message,
      messageId: 'invalid-clock-test',
      lamportTimestamp: -1
    })).rejects.toThrow();
    expect(receiverClock.getTimestamp()).toBe(8);
  });
});

describe('Gateway global-state aggregation', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('reports every healthy department online with identity and priority', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input) => {
      const url = new URL(input);
      const department = url.port === '5001' ? 'CS' : url.port === '5002' ? 'IT' : 'ENTC';
      const priorities = { CS: 1, IT: 2, ENTC: 3 };
      const payload = url.pathname === '/internal/health'
        ? { department, nodeId: department, status: 'healthy', lastHeartbeat: '2025-01-01T00:00:00.000Z', lamportClock: priorities[department] }
        : {
          department,
          priority: priorities[department],
          coordinator: 'ENTC',
          leaderPriority: 3,
          electionStatus: 'coordinator_announced'
        };
      return { ok: true, json: async () => payload };
    }));

    const { body } = await request(app).get('/api/nodes').expect(200);
    expect(body.map((node) => [node.nodeId, node.status, node.priority])).toEqual([
      ['CS', 'online', 1],
      ['IT', 'online', 2],
      ['ENTC', 'online', 3]
    ]);
    expect(body.every((node) => node.leader === 'ENTC' && node.leaderPriority === 3)).toBe(true);
  });

  it('keeps a responding node with missing/unknown health marked unknown', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input) => {
      const url = new URL(input);
      const payload = url.pathname === '/internal/health'
        ? { department: 'CS', nodeId: 'CS', status: 'unknown' }
        : { department: 'CS', coordinator: 'CS', priority: 1, leaderPriority: 1 };
      return { ok: true, json: async () => payload };
    }));

    const { body } = await request(app).get('/api/nodes').expect(200);
    expect(body[0].status).toBe('unknown');
    expect(body[0].leader).toBe('CS');
  });

  it('reports online, timeout/unknown, and offline nodes independently', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input) => {
      const url = new URL(input);
      const department = url.port === '5001' ? 'CS' : url.port === '5002' ? 'IT' : 'ENTC';
      if (department === 'IT') {
        throw Object.assign(new Error('request timed out'), { name: 'TimeoutError' });
      }
      if (department === 'ENTC') {
        throw Object.assign(new TypeError('connect failed'), { cause: { code: 'ECONNREFUSED' } });
      }
      const payload = url.pathname === '/internal/health'
        ? { department, nodeId: department, status: 'healthy', lastHeartbeat: '2025-01-01T00:00:00.000Z', lamportClock: 7 }
        : { department, priority: 1, coordinator: 'CS', leaderPriority: 1, electionStatus: 'coordinator_announced' };
      return { ok: true, json: async () => payload };
    }));

    const { body } = await request(app).get('/api/nodes').expect(200);
    expect(body.map((node) => [node.department, node.status])).toEqual([
      ['CS', 'online'],
      ['IT', 'unknown'],
      ['ENTC', 'offline']
    ]);
    expect(body[0].lamportClock).toBe(7);
    expect(body[1].healthError).toContain('timed out');
    expect(body[2].healthError).toContain('connect failed');
  });

  it('reports no available leader when every department is unreachable', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => {
      throw Object.assign(new TypeError('connect failed'), { cause: { code: 'ECONNREFUSED' } });
    }));

    const { body } = await request(app).get('/api/nodes').expect(200);
    expect(body).toHaveLength(3);
    expect(body.every((node) => node.status === 'offline' && node.leader === null)).toBe(true);
  });

  it('forwards an explicit election trigger to one department service', async () => {
    const fetch = vi.fn(async () => ({
      ok: true,
      json: async () => ({ coordinator: 'ENTC', leaderPriority: 3, electionStatus: 'coordinator_announced' })
    }));
    vi.stubGlobal('fetch', fetch);

    const { body } = await request(app).post('/api/distributed/election').expect(200);
    expect(body.triggerNode).toBe('ENTC');
    expect(body.result.coordinator).toBe('ENTC');
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(String(fetch.mock.calls[0][0])).toContain('/internal/election');
  });
});
