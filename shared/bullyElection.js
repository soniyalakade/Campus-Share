import { DEPARTMENTS } from './constants.js';

export const DEPARTMENT_PRIORITIES = Object.freeze({ CS: 1, IT: 2, ENTC: 3 });

const addBounded = (set, value, limit = 1000) => {
  if (set.has(value)) return false;
  set.add(value);
  if (set.size > limit) set.delete(set.values().next().value);
  return true;
};

export class BullyElection {
  constructor({
    department,
    clock,
    sendMessage,
    checkPeer,
    timeoutMs = 1200,
    coordinatorWaitMs = 3000,
    retryDelayMs = 200
  }) {
    if (!DEPARTMENTS.includes(department)) throw new Error(`Unknown department: ${department}`);
    this.department = department;
    this.clock = clock;
    this.sendMessage = sendMessage;
    this.checkPeer = checkPeer;
    this.timeoutMs = timeoutMs;
    this.coordinatorWaitMs = coordinatorWaitMs;
    this.retryDelayMs = retryDelayMs;
    this.coordinator = null;
    this.electionStatus = 'initializing';
    this.electionId = null;
    this.latestResult = null;
    this.lastElectionAt = null;
    this.sequence = 0;
    this.activeElection = null;
    this.receivedMessages = new Set();
    this.peerAvailability = Object.fromEntries(DEPARTMENTS.map((node) => [node, 'unknown']));
  }

  getState() {
    return {
      department: this.department,
      nodeId: this.department,
      priority: DEPARTMENT_PRIORITIES[this.department],
      coordinator: this.coordinator,
      leaderPriority: this.coordinator ? DEPARTMENT_PRIORITIES[this.coordinator] : null,
      electionStatus: this.electionStatus,
      electionId: this.electionId,
      latestResult: this.latestResult,
      lastElectionAt: this.lastElectionAt,
      peerAvailability: { ...this.peerAvailability }
    };
  }

  async receiveMessage(message) {
    const { type, sender, messageId, electionId, lamportTimestamp } = message || {};
    if (!['ELECTION', 'COORDINATOR'].includes(type)
      || !DEPARTMENTS.includes(sender)
      || sender === this.department
      || typeof messageId !== 'string'
      || !messageId
      || typeof electionId !== 'string'
      || !electionId
      || !Number.isSafeInteger(lamportTimestamp)
      || lamportTimestamp < 0) {
      throw new TypeError('Invalid Bully election message');
    }
    if (!addBounded(this.receivedMessages, messageId)) {
      return {
        ...this.getState(),
        accepted: true,
        duplicate: true,
        ...(type === 'ELECTION' ? { response: 'OK' } : {}),
        sender: this.department,
        messageId: `${messageId}:duplicate-ack`,
        electionId,
        lamportTimestamp: this.clock.tick()
      };
    }
    this.clock.receive(lamportTimestamp);

    if (type === 'ELECTION') {
      if (DEPARTMENT_PRIORITIES[sender] >= DEPARTMENT_PRIORITIES[this.department]) {
        return {
          accepted: false,
          reason: 'Election requests must come from a lower-priority node',
          response: 'REJECTED',
          sender: this.department,
          messageId: `${messageId}:rejected`,
          electionId,
          lamportTimestamp: this.clock.tick()
        };
      }
      const response = {
        accepted: true,
        response: 'OK',
        sender: this.department,
        messageId: `${messageId}:ok`,
        electionId,
        lamportTimestamp: this.clock.tick()
      };
      setTimeout(() => {
        this.startElection('higher-priority-node-request').catch((error) => {
          this.electionStatus = 'failed';
          this.latestResult = { success: false, error: error.message };
        });
      }, 0);
      return response;
    }

    if (this.coordinator && DEPARTMENT_PRIORITIES[this.coordinator] > DEPARTMENT_PRIORITIES[sender]) {
      try {
        const currentLeaderAvailable = await this.checkPeer(this.coordinator, this.timeoutMs);
        this.peerAvailability[this.coordinator] = currentLeaderAvailable ? 'online' : 'offline';
        if (currentLeaderAvailable) {
          return {
            accepted: false,
            reason: 'A lower-priority node cannot replace an available current leader',
            sender: this.department,
            messageId: `${messageId}:ack`,
            electionId,
            lamportTimestamp: this.clock.tick(),
            ...this.getState()
          };
        }
      } catch (_error) {
        this.peerAvailability[this.coordinator] = 'unknown';
        return {
          accepted: false,
          reason: 'Current leader availability is unknown',
          sender: this.department,
          messageId: `${messageId}:ack`,
          electionId,
          lamportTimestamp: this.clock.tick(),
          ...this.getState()
        };
      }
      this.coordinator = null;
    }
    this.coordinator = sender;
    this.electionId = electionId;
    this.electionStatus = 'coordinator_announced';
    this.latestResult = { success: true, coordinator: sender, electionId };
    return {
      accepted: true,
      duplicate: false,
      sender: this.department,
      messageId: `${messageId}:ack`,
      electionId,
      lamportTimestamp: this.clock.tick(),
      ...this.getState()
    };
  }

  startElection(reason = 'manual') {
    if (this.activeElection) return this.activeElection;
    this.activeElection = this.#runElection(reason).finally(() => {
      this.activeElection = null;
    });
    return this.activeElection;
  }

  async #runElection(reason) {
    this.electionStatus = 'electing';
    this.lastElectionAt = new Date().toISOString();
    this.sequence += 1;
    const electionId = `${this.department}-${this.sequence}`;
    this.electionId = electionId;
    const higherNodes = DEPARTMENTS.filter((node) => (
      DEPARTMENT_PRIORITIES[node] > DEPARTMENT_PRIORITIES[this.department]
    ));

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const responses = await Promise.allSettled(higherNodes.map(async (node) => {
        const messageId = `${electionId}:election:${node}:${attempt}`;
        const result = await this.sendMessage(node, '/internal/election/message', {
          type: 'ELECTION',
          sender: this.department,
          messageId,
          electionId,
          lamportTimestamp: this.clock.tick()
        }, this.timeoutMs);
        this.clock.receive(result?.lamportTimestamp);
        this.peerAvailability[node] = 'online';
        return result?.response === 'OK';
      }));
      responses.forEach((result, index) => {
        if (result.status === 'rejected') this.peerAvailability[higherNodes[index]] = 'unknown';
      });

      const higherResponded = responses.some((result) => result.status === 'fulfilled' && result.value);
      if (!higherResponded) {
        const currentPriority = this.coordinator ? DEPARTMENT_PRIORITIES[this.coordinator] : 0;
        if (currentPriority > DEPARTMENT_PRIORITIES[this.department]) {
          this.electionStatus = 'coordinator_announced';
          this.latestResult = { success: true, coordinator: this.coordinator, electionId, reason };
          return this.getState();
        }
        await this.#announceCoordinator(electionId, reason);
        return this.getState();
      }

      this.electionStatus = 'waiting_for_coordinator';
      const completed = await this.#waitForHigherCoordinator();
      if (completed) {
        this.electionStatus = 'coordinator_announced';
        this.latestResult = { success: true, coordinator: this.coordinator, electionId, reason };
        return this.getState();
      }
      if (attempt === 0) await new Promise((resolve) => setTimeout(resolve, this.retryDelayMs));
    }

    this.electionStatus = 'failed';
    this.latestResult = { success: false, coordinator: null, electionId, reason, error: 'Higher-priority nodes responded but no coordinator announcement arrived' };
    return this.getState();
  }

  async #waitForHigherCoordinator() {
    const deadline = Date.now() + this.coordinatorWaitMs;
    while (Date.now() < deadline) {
      if (this.coordinator
        && DEPARTMENT_PRIORITIES[this.coordinator] > DEPARTMENT_PRIORITIES[this.department]) return true;
      await new Promise((resolve) => setTimeout(resolve, Math.min(this.retryDelayMs, Math.max(1, deadline - Date.now()))));
    }
    return Boolean(this.coordinator
      && DEPARTMENT_PRIORITIES[this.coordinator] > DEPARTMENT_PRIORITIES[this.department]);
  }

  async #announceCoordinator(electionId, reason) {
    const currentPriority = this.coordinator ? DEPARTMENT_PRIORITIES[this.coordinator] : 0;
    if (currentPriority > DEPARTMENT_PRIORITIES[this.department]) return;
    this.coordinator = this.department;
    this.electionStatus = 'coordinator_announced';
    this.latestResult = { success: true, coordinator: this.department, electionId, reason };
    const message = {
      type: 'COORDINATOR',
      sender: this.department,
      messageId: `${electionId}:coordinator:${this.department}`,
      electionId,
      lamportTimestamp: this.clock.tick()
    };
    await Promise.allSettled(DEPARTMENTS.filter((node) => node !== this.department).map(async (node) => {
      try {
        const response = await this.sendMessage(node, '/internal/election/message', message, this.timeoutMs);
        this.clock.receive(response?.lamportTimestamp);
        this.peerAvailability[node] = 'online';
      } catch (_error) {
        this.peerAvailability[node] = 'unknown';
      }
    }));
  }

  async checkPeers() {
    const states = await Promise.allSettled(DEPARTMENTS.filter((node) => node !== this.department).map(async (node) => {
      const reachable = await this.checkPeer(node, this.timeoutMs);
      this.peerAvailability[node] = reachable ? 'online' : 'offline';
      return { node, reachable };
    }));
    states.forEach((result, index) => {
      if (result.status === 'rejected') {
        const node = DEPARTMENTS.filter((item) => item !== this.department)[index];
        this.peerAvailability[node] = 'unknown';
      }
    });
    const availablePeers = states
      .filter((result) => result.status === 'fulfilled' && result.value.reachable)
      .map((result) => result.value.node);
    const leaderIsAvailable = this.coordinator === this.department
      || (this.coordinator && availablePeers.includes(this.coordinator));
    const higherNodeRecovered = Boolean(this.coordinator && availablePeers.some((node) => (
      DEPARTMENT_PRIORITIES[node] > DEPARTMENT_PRIORITIES[this.coordinator]
    )));
    const electionReason = !this.coordinator
      ? 'startup-or-no-leader'
      : !leaderIsAvailable
        ? 'leader-unreachable'
        : higherNodeRecovered
          ? 'higher-priority-node-recovered'
          : null;
    if (!electionReason) return this.getState();
    if (!leaderIsAvailable) this.coordinator = null;
    return this.startElection(electionReason);
  }
}
