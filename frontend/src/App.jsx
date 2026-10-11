import { useCallback, useEffect, useRef, useState } from 'react';
import { Link, Navigate, Route, Routes, useNavigate } from 'react-router-dom';

async function api(path, { token, ...options } = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(options.headers || {})
    }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(payload.error || `Request failed (${response.status})`);
    error.nodesUnreachable = payload.nodesUnreachable || [];
    throw error;
  }
  return payload;
}

const lifecycleEventTypes = new Set([
  'RESOURCE_ADDED',
  'RESOURCE_UPDATED',
  'RESOURCE_DELETED',
  'RESOURCE_RESERVED',
  'RESOURCE_APPROVED',
  'RESOURCE_REJECTED',
  'RESOURCE_CANCELLED',
  'RESOURCE_BORROWED',
  'RESOURCE_RETURNED'
]);

const eventIdFor = (event) => event?.eventId || event?.id;

const addLimitedEventId = (ids, eventId) => {
  if (!eventId || ids.has(eventId)) return false;
  ids.add(eventId);
  if (ids.size > 1000) ids.delete(ids.values().next().value);
  return true;
};

async function getResourceNames(records) {
  const resourceIds = [...new Set(records.map((record) => record.resourceId).filter(Boolean))];
  const entries = await Promise.all(resourceIds.map(async (resourceId) => {
    try {
      const resource = await api(`/api/resources/${encodeURIComponent(resourceId)}`);
      return [resourceId, resource.name];
    } catch (_error) {
      return [resourceId, resourceId];
    }
  }));
  return new Map(entries);
}

const formatTimestamp = (value) => {
  if (!value) return '';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '' : date.toLocaleString();
};

const newestFirst = (records) => [...records].sort((left, right) => {
  const leftTime = new Date(left.updatedAt || left.returnedAt || left.borrowedAt || left.createdAt || 0).getTime();
  const rightTime = new Date(right.updatedAt || right.returnedAt || right.borrowedAt || right.createdAt || 0).getTime();
  return rightTime - leftTime;
});

const reservationAction = (status) => ({
  PENDING: 'Reservation requested',
  APPROVED: 'Reservation approved',
  REJECTED: 'Reservation rejected',
  CANCELLED: 'Reservation cancelled',
  FULFILLED: 'Reservation checked out'
}[status] || 'Reservation');

const statusLabel = (status) => String(status || '')
  .toLowerCase()
  .replaceAll('_', ' ')
  .replace(/\b\w/g, (character) => character.toUpperCase());

function App() {
  const navigate = useNavigate();
  const [session, setSession] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem('campus-share-session') || 'null');
    } catch (_error) {
      return null;
    }
  });
  const [nodes, setNodes] = useState([]);
  const [nodeError, setNodeError] = useState('');
  const [connected, setConnected] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [notice, setNotice] = useState('');
  const refreshedEventIds = useRef(new Set());

  const onSession = (value) => {
    setSession(value);
    setNotice('');
    if (value) localStorage.setItem('campus-share-session', JSON.stringify(value));
    else localStorage.removeItem('campus-share-session');
    navigate('/');
  };

  const refreshNodes = useCallback(async () => {
    try {
      setNodes(await api('/api/nodes'));
      setNodeError('');
    } catch (error) {
      setNodeError(error.message);
    }
  }, []);

  useEffect(() => {
    refreshNodes();
    const timer = setInterval(refreshNodes, 5000);
    return () => clearInterval(timer);
  }, [refreshNodes]);

  useEffect(() => {
    const wsUrl = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws/updates`;
    const reconnectBaseDelay = 1000;
    const reconnectMaxDelay = 30000;
    let socket;
    let reconnectTimer;
    let retryAttempt = 0;
    let active = true;

    const connect = () => {
      if (!active || (socket && socket.readyState < WebSocket.CLOSING)) return;
      const ws = new WebSocket(wsUrl);
      socket = ws;
      ws.onopen = () => {
        if (!active || socket !== ws) return;
        retryAttempt = 0;
        setConnected(true);
      };
      ws.onclose = () => {
        if (socket !== ws) return;
        socket = null;
        setConnected(false);
        if (!active) return;
        const delay = Math.min(reconnectBaseDelay * (2 ** retryAttempt), reconnectMaxDelay);
        retryAttempt += 1;
        reconnectTimer = setTimeout(connect, delay);
      };
      ws.onerror = () => {
        if (socket === ws) setConnected(false);
      };
      ws.onmessage = (message) => {
        try {
          const payload = JSON.parse(message.data);
          if (payload.type === 'CONNECTED') {
            const history = Array.isArray(payload.events) ? payload.events : [];
            for (const event of history) {
              const eventId = eventIdFor(event);
              if (eventId && lifecycleEventTypes.has(event?.eventType)) {
                addLimitedEventId(refreshedEventIds.current, eventId);
              }
            }
            setRefreshKey((value) => value + 1);
          } else if (payload.type === 'EVENT' && payload.event) {
            const eventId = eventIdFor(payload.event);
            if (lifecycleEventTypes.has(payload.event.eventType)
              && (!eventId || addLimitedEventId(refreshedEventIds.current, eventId))) {
              setRefreshKey((value) => value + 1);
            }
          }
        } catch (_error) {
          setNotice('A malformed update event was ignored.');
        }
      };
    };

    connect();
    return () => {
      active = false;
      clearTimeout(reconnectTimer);
      if (socket && socket.readyState < WebSocket.CLOSING) socket.close();
    };
  }, [refreshNodes]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <h1>Campus Share</h1>
          <span>Distributed resource network</span>
        </div>
        <nav>
          <Link to="/">Overview</Link>
          {session?.user.role === 'STUDENT' && <Link to="/activity">My Activity</Link>}
          {session?.user.role === 'DEPARTMENT_ADMIN' && (
            <>
              <Link to="/requests">Requests</Link>
              <Link to="/resource-management">Resource Management</Link>
            </>
          )}
          <Link to="/monitor">Node Monitor</Link>
        </nav>
        <AuthPanel session={session} onSession={onSession} />
      </aside>

      <main className="content">
        {notice && <div className="notice error" role="alert">{notice}</div>}
        <Routes>
          <Route path="/" element={
            <Overview
              connected={connected}
              refreshKey={refreshKey}
              session={session}
            />
          } />
          <Route path="/activity" element={session?.user.role === 'STUDENT'
            ? <StudentActivity session={session} refreshKey={refreshKey} onLifecycleSuccess={() => setRefreshKey((value) => value + 1)} />
            : <Navigate to="/" replace />} />
          <Route path="/requests" element={session?.user.role === 'DEPARTMENT_ADMIN'
            ? <AdminView screen="requests" session={session} refreshKey={refreshKey} onLifecycleSuccess={() => setRefreshKey((value) => value + 1)} />
            : <Navigate to="/" replace />} />
          <Route path="/resource-management" element={session?.user.role === 'DEPARTMENT_ADMIN'
            ? <AdminView screen="resources" session={session} refreshKey={refreshKey} onLifecycleSuccess={() => setRefreshKey((value) => value + 1)} />
            : <Navigate to="/" replace />} />
          <Route path="/students" element={<Navigate to={session?.user.role === 'STUDENT' ? '/activity' : '/'} replace />} />
          <Route path="/admins" element={<Navigate to={session?.user.role === 'DEPARTMENT_ADMIN' ? '/requests' : '/'} replace />} />
          <Route path="/monitor" element={<MonitorView nodes={nodes} nodeError={nodeError} refreshNodes={refreshNodes} />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}

function AuthPanel({ session, onSession }) {
  const [mode, setMode] = useState('login');
  const [role, setRole] = useState('STUDENT');
  const [department, setDepartment] = useState('CS');
  const [adminRegistrationKey, setAdminRegistrationKey] = useState('');
  const [form, setForm] = useState({ name: '', email: '', password: '' });
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async (event) => {
    event.preventDefault();
    setError('');
    setBusy(true);
    try {
      const path = mode === 'register' ? '/api/auth/register' : '/api/auth/login';
      const body = mode === 'register'
        ? {
          ...form,
          role,
          ...(role === 'DEPARTMENT_ADMIN' ? { department, adminRegistrationKey } : {})
        }
        : { email: form.email, password: form.password };
      const result = await api(path, { method: 'POST', body: JSON.stringify(body) });
      onSession({ token: result.token, user: result.user });
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusy(false);
    }
  };

  if (session) {
    return (
      <section className="auth-card">
        <p className="eyebrow">Signed in</p>
        <strong>{session.user.name}</strong>
        <span>{session.user.role}{session.user.department ? ` · ${session.user.department}` : ''}</span>
        <button className="button secondary" onClick={() => onSession(null)}>Sign out</button>
      </section>
    );
  }

  return (
    <section className="auth-card">
      <p className="eyebrow">{mode === 'login' ? 'Welcome back' : 'Create account'}</p>
      <form onSubmit={submit}>
        {mode === 'register' && (
          <>
            <label>Name<input required value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} /></label>
            <label>Account type
              <select value={role} onChange={(event) => setRole(event.target.value)}>
                <option value="STUDENT">Student</option>
                <option value="DEPARTMENT_ADMIN">Department administrator</option>
              </select>
            </label>
            {role === 'DEPARTMENT_ADMIN' && (
              <>
                <label>Department
                  <select value={department} onChange={(event) => setDepartment(event.target.value)}>
                    <option>CS</option><option>IT</option><option>ENTC</option>
                  </select>
                </label>
                <label>Administrator registration key
                  <input type="password" required value={adminRegistrationKey} onChange={(event) => setAdminRegistrationKey(event.target.value)} />
                </label>
              </>
            )}
          </>
        )}
        <label>Email<input type="email" required value={form.email} onChange={(event) => setForm({ ...form, email: event.target.value })} /></label>
        <label>Password<input type="password" required minLength="8" value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} /></label>
        {error && <p className="notice error" role="alert">{error}</p>}
        <button className="button" type="submit" disabled={busy}>{busy ? 'Please wait…' : mode === 'login' ? 'Sign in' : 'Register'}</button>
      </form>
      <button className="link-button" onClick={() => { setMode(mode === 'login' ? 'register' : 'login'); setError(''); }}>
        {mode === 'login' ? 'Create an account' : 'Already registered? Sign in'}
      </button>
    </section>
  );
}

function Overview({ connected, refreshKey, session }) {
  const [query, setQuery] = useState('');
  const [resources, setResources] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [nodesUnreachable, setNodesUnreachable] = useState([]);
  const [requestSequence, setRequestSequence] = useState(0);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    api('/api/resources')
      .then((data) => {
        if (active) {
          setResources(Array.isArray(data.resources) ? data.resources : []);
          setNodesUnreachable(Array.isArray(data.nodesUnreachable) ? data.nodesUnreachable : []);
        }
      })
      .catch((requestError) => {
        if (active) {
          setResources([]);
          setNodesUnreachable(requestError.nodesUnreachable || []);
          setError(requestError.message);
        }
      })
      .finally(() => {
        if (active) setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [refreshKey, requestSequence]);

  const normalizedQuery = query.trim().toLocaleLowerCase();
  const visibleResources = resources.filter((resource) => (
    !normalizedQuery || String(resource.name || '').toLocaleLowerCase().includes(normalizedQuery)
  ));

  return (
    <>
      <section className="hero">
        <div>
          <p className="eyebrow">Campus operations</p>
          <h2>Campus resource catalogue</h2>
          <p>Discover resources owned by CS, IT, and ENTC department services.</p>
        </div>
        <div className={`status-badge ${connected ? 'live' : 'offline'}`}>{connected ? 'Live updates' : 'Updates offline'}</div>
      </section>
      <section className="table-card">
        <h3>All resources</h3>
        <label htmlFor="overview-search">Search by resource name</label>
        <input
          id="overview-search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search resources by name..."
        />
        <button className="button secondary" type="button" disabled={loading} onClick={() => setRequestSequence((value) => value + 1)}>
          {loading ? 'Loading…' : 'Refresh catalogue'}
        </button>
        {loading && <p>Loading resources from department services…</p>}
        {nodesUnreachable.length > 0 && (
          <p className="notice" role="status">Catalogue is incomplete; unavailable departments: {nodesUnreachable.join(', ')}.</p>
        )}
        {error && <p className="notice error" role="alert">{error}</p>}
        {!loading && !error && visibleResources.length > 0 ? (
          <ResourceTable
            resources={visibleResources}
            token={session?.token}
            role={session?.user.role}
            onSuccess={(result) => {
              if (result?.resource) {
                setResources((current) => current.map((resource) => (
                  resource.resourceId === result.resource.resourceId ? result.resource : resource
                )));
              }
            }}
          />
        ) : null}
        {!loading && !error && visibleResources.length === 0 && resources.length === 0 && (
          <p>No resources are currently listed in the reachable departments.</p>
        )}
        {!loading && !error && visibleResources.length === 0 && resources.length > 0 && (
          <p>No resources match “{query.trim()}”.</p>
        )}
      </section>
    </>
  );
}

function ResourceTable({ resources, token, role, onSuccess }) {
  const [busyId, setBusyId] = useState('');
  const [detailsId, setDetailsId] = useState('');
  const [details, setDetails] = useState(null);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [message, setMessage] = useState('');
  const showDetails = async (resource) => {
    if (detailsId === resource.resourceId) {
      setDetailsId('');
      setDetails(null);
      return;
    }
    setDetailsId(resource.resourceId);
    setDetails(null);
    setDetailsLoading(true);
    try {
      setDetails(await api(`/api/resources/${encodeURIComponent(resource.resourceId)}`));
    } catch (error) {
      setMessage(error.message);
    } finally {
      setDetailsLoading(false);
    }
  };
  const reserve = async (resource) => {
    if (!token || role !== 'STUDENT') {
      setMessage(token ? 'Only student accounts can request reservations.' : 'Sign in as a student to request a reservation.');
      return;
    }
    setBusyId(resource.resourceId);
    setMessage('');
    try {
      const result = await api('/api/reservations', { token, method: 'POST', body: JSON.stringify({ resourceId: resource.resourceId }) });
      setMessage('Reservation request submitted for department approval.');
      await onSuccess?.(result);
    } catch (error) {
      setMessage(error.message);
    } finally {
      setBusyId('');
    }
  };
  return (
    <>
      {message && <p className="notice" role="status">{message}</p>}
      <div className="resource-grid">
        {resources.map((resource) => (
          <article className="resource-card" key={resource.resourceId}>
            <span className="resource-department">{resource.department} · {resource.laboratory}</span>
            <h4>{resource.name}</h4>
            <p>{resource.category}</p>
            <p>{resource.availableQuantity} available of {resource.totalQuantity}</p>
            {resource.condition && <p>Condition: {resource.condition}</p>}
            {resource.description && <p>{resource.description}</p>}
            <div className="stack">
              <button className="button secondary" onClick={() => showDetails(resource)}>{detailsId === resource.resourceId ? 'Hide details' : 'Details'}</button>
              {role === 'STUDENT' && resource.availableQuantity > 0 && (
                <button className="button" disabled={busyId === resource.resourceId} onClick={() => reserve(resource)}>
                  {busyId === resource.resourceId ? 'Submitting…' : 'Reserve'}
                </button>
              )}
            </div>
            {detailsId === resource.resourceId && (
              <div className="resource-details">
                {detailsLoading ? <p>Loading details…</p> : details ? (
                  <>
                    <p>{details.description || 'No description provided.'}</p>
                    <p>Condition: {details.condition} · Reserved: {details.reservedQuantity} · Borrowed: {details.borrowedQuantity}</p>
                    {details.manualUrl && <a href={details.manualUrl} target="_blank" rel="noreferrer">Manual</a>}
                    {details.tutorialVideoUrl && <a href={details.tutorialVideoUrl} target="_blank" rel="noreferrer">Tutorial video</a>}
                  </>
                ) : <p>Details unavailable.</p>}
              </div>
            )}
          </article>
        ))}
      </div>
    </>
  );
}

function StudentActivity({ session, refreshKey, onLifecycleSuccess }) {
  const [reservations, setReservations] = useState([]);
  const [borrowRecords, setBorrowRecords] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busyId, setBusyId] = useState('');
  const refreshSequence = useRef(0);
  const token = session?.token;

  const refreshMine = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    if (!token) {
      setReservations([]);
      setBorrowRecords([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    try {
      const [reservationData, borrowData] = await Promise.all([
        api('/api/reservations/my', { token }),
        api('/api/borrow-records/my', { token })
      ]);
      const names = await getResourceNames([
        ...reservationData.reservations,
        ...borrowData.borrowRecords
      ]);
      if (sequence !== refreshSequence.current) return;
      setReservations(reservationData.reservations.map((item) => ({
        ...item,
        resourceName: names.get(item.resourceId) || item.resourceId
      })));
      setBorrowRecords(borrowData.borrowRecords.map((item) => ({
        ...item,
        resourceName: names.get(item.resourceId) || item.resourceId
      })));
      const unavailableDepartments = [...new Set([
        ...(reservationData.nodesUnreachable || []),
        ...(borrowData.nodesUnreachable || [])
      ])];
      setError(unavailableDepartments.length
        ? `Some department services are unavailable; your history may be incomplete: ${unavailableDepartments.join(', ')}.`
        : '');
    } catch (requestError) {
      if (sequence === refreshSequence.current) setError(requestError.message);
    } finally {
      if (sequence === refreshSequence.current) setLoading(false);
    }
  }, [token]);

  useEffect(() => { refreshMine(); }, [refreshMine, refreshKey]);

  const runAction = async (id, action, operation) => {
    setBusyId(id);
    setError('');
    setNotice('');
    try {
      await operation();
      setNotice(`${action} completed.`);
      onLifecycleSuccess();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyId('');
    }
  };

  return (
    <section className="panel">
      <h2>My Activity</h2>
      {loading && <p>Loading your reservation and borrowing activity…</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      {notice && <p className="notice" role="status">{notice}</p>}
      <div className="lifecycle-columns">
        <section>
          <h3>Reservation history</h3>
          {!loading && reservations.length === 0 ? <p>No reservations yet.</p> : newestFirst(reservations).map((item) => (
            <article className="list-card" key={item.reservationId}>
              <strong>{item.resourceName || item.resourceId}</strong>
              {item.resourceName && item.resourceName !== item.resourceId && <small>{item.resourceId}</small>}
              <span>{item.department} · Quantity: 1 · {reservationAction(item.status)} · Status: {statusLabel(item.status)}</span>
              {item.createdAt && <small>Requested {formatTimestamp(item.createdAt)}</small>}
              {item.updatedAt && item.status !== 'PENDING' && <small>Updated {formatTimestamp(item.updatedAt)}</small>}
              {item.status === 'PENDING' && <small>Borrowing becomes available after department approval.</small>}
              {['PENDING', 'APPROVED'].includes(item.status) && (
                <div className="stack">
                  {item.status === 'APPROVED' && <button className="button" disabled={busyId === item.reservationId} onClick={() => runAction(item.reservationId, 'Borrow', () => api(`/api/resources/${encodeURIComponent(item.resourceId)}/borrow`, { token, method: 'POST', body: JSON.stringify({ reservationId: item.reservationId }) }))}>Borrow</button>}
                  <button className="button secondary" disabled={busyId === item.reservationId} onClick={() => runAction(item.reservationId, 'Cancellation', () => api(`/api/reservations/${encodeURIComponent(item.reservationId)}`, { token, method: 'DELETE' }))}>Cancel</button>
                </div>
              )}
            </article>
          ))}
        </section>
        <section>
          <h3>Borrow history</h3>
          {!loading && borrowRecords.length === 0 ? <p>No borrow records yet.</p> : newestFirst(borrowRecords).map((item) => (
            <article className="list-card" key={item.borrowRecordId}>
              <strong>{item.resourceName || item.resourceId}</strong>
              {item.resourceName && item.resourceName !== item.resourceId && <small>{item.resourceId}</small>}
              <span>{item.department} · Quantity: 1 · Status: {statusLabel(item.status)}</span>
              {item.borrowedAt && <small>Borrowed {formatTimestamp(item.borrowedAt)}</small>}
              {item.returnedAt && <small>Returned {formatTimestamp(item.returnedAt)}</small>}
              {item.status === 'BORROWED' && <button className="button" disabled={busyId === item.borrowRecordId} onClick={() => runAction(item.borrowRecordId, 'Return', () => api(`/api/resources/${encodeURIComponent(item.resourceId)}/return`, { token, method: 'POST', body: JSON.stringify({ borrowRecordId: item.borrowRecordId }) }))}>Return</button>}
            </article>
          ))}
        </section>
      </div>
    </section>
  );
}

function AdminView({ screen, session, refreshKey, onLifecycleSuccess }) {
  const [resources, setResources] = useState([]);
  const [reservations, setReservations] = useState([]);
  const [borrowRecords, setBorrowRecords] = useState([]);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busyId, setBusyId] = useState('');
  const [loading, setLoading] = useState(true);
  const [resourceBusy, setResourceBusy] = useState(false);
  const [resourcesUnavailable, setResourcesUnavailable] = useState(false);
  const [resourceForm, setResourceForm] = useState({
    name: '',
    category: '',
    description: '',
    laboratory: '',
    condition: '',
    totalQuantity: '1'
  });
  const [resourceDrafts, setResourceDrafts] = useState({});
  const refreshSequence = useRef(0);

  const refresh = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    if (session?.user.role !== 'DEPARTMENT_ADMIN') {
      setReservations([]);
      setBorrowRecords([]);
      setResources([]);
      return;
    }
    setLoading(true);
    try {
      const [resourceData, reservationData, borrowData] = await Promise.all([
        api('/api/resources', { token: session.token }),
        api('/api/reservations', { token: session.token }),
        api('/api/borrow-records/my', { token: session.token })
      ]);
      const names = await getResourceNames([
        ...reservationData.reservations,
        ...borrowData.borrowRecords
      ]);
      if (sequence !== refreshSequence.current) return;
      setResources((resourceData.resources || []).filter((item) => item.department === session.user.department && item.resourceId));
      setResourcesUnavailable((resourceData.nodesUnreachable || []).includes(session.user.department));
      setReservations(reservationData.reservations.map((item) => ({
        ...item,
        resourceName: names.get(item.resourceId) || item.resourceId
      })));
      setBorrowRecords(borrowData.borrowRecords.map((item) => ({
        ...item,
        resourceName: names.get(item.resourceId) || item.resourceId
      })));
      const unavailableDepartments = [...new Set([
        ...(resourceData.nodesUnreachable || []),
        ...(reservationData.nodesUnreachable || []),
        ...(borrowData.nodesUnreachable || [])
      ])];
      setError(unavailableDepartments.length
        ? `Some department services are unavailable; displayed catalogue or records may be incomplete: ${unavailableDepartments.join(', ')}.`
        : '');
    } catch (requestError) {
      if (sequence === refreshSequence.current) {
        setResourcesUnavailable(true);
        setError(requestError.message);
      }
    } finally {
      if (sequence === refreshSequence.current) setLoading(false);
    }
  }, [session]);

  useEffect(() => { refresh(); }, [refresh, refreshKey]);

  const afterResourceChange = (message) => {
    setNotice(message);
    setError('');
    onLifecycleSuccess();
  };

  const createResource = async (event) => {
    event.preventDefault();
    setResourceBusy(true);
    setError('');
    setNotice('');
    const quantity = Number(resourceForm.totalQuantity);
    try {
      await api('/api/resources', {
        token: session.token,
        method: 'POST',
        body: JSON.stringify({
          name: resourceForm.name.trim(),
          category: resourceForm.category.trim(),
          description: resourceForm.description.trim(),
          laboratory: resourceForm.laboratory.trim() || 'Main Lab',
          condition: resourceForm.condition.trim() || 'Good',
          totalQuantity: quantity,
          availableQuantity: quantity
        })
      });
      setResourceForm({ name: '', category: '', description: '', laboratory: '', condition: '', totalQuantity: '1' });
      await afterResourceChange('Resource added to your department.');
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setResourceBusy(false);
    }
  };

  const updateResource = async (resource) => {
    const draft = resourceDrafts[resource.resourceId] || resource;
    const availableQuantity = Number(draft.availableQuantity);
    if (!String(draft.name || '').trim() || !String(draft.category || '').trim()) {
      setError('Resource name and category are required.');
      return;
    }
    if (!Number.isInteger(availableQuantity) || availableQuantity < 0) {
      setError('Available quantity must be a non-negative whole number.');
      return;
    }
    setResourceBusy(true);
    setError('');
    setNotice('');
    try {
      await api(`/api/resources/${encodeURIComponent(resource.resourceId)}`, {
        token: session.token,
        method: 'PUT',
        body: JSON.stringify({
          name: String(draft.name || '').trim(),
          category: String(draft.category || '').trim(),
          description: String(draft.description || '').trim(),
          laboratory: String(draft.laboratory || '').trim(),
          condition: String(draft.condition || '').trim(),
          availableQuantity,
          totalQuantity: availableQuantity + resource.reservedQuantity + resource.borrowedQuantity
        })
      });
      setResourceDrafts((current) => {
        const next = { ...current };
        delete next[resource.resourceId];
        return next;
      });
      await afterResourceChange(`${resource.name} quantity updated.`);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setResourceBusy(false);
    }
  };

  const deleteResource = async (resource) => {
    setResourceBusy(true);
    setError('');
    setNotice('');
    try {
      await api(`/api/resources/${encodeURIComponent(resource.resourceId)}`, {
        token: session.token,
        method: 'DELETE'
      });
      await afterResourceChange(`${resource.name} deleted.`);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setResourceBusy(false);
    }
  };

  const updateStatus = async (reservation, status) => {
    setBusyId(reservation.reservationId);
    setError('');
    setNotice('');
    try {
      await api(`/api/reservations/${encodeURIComponent(reservation.reservationId)}/status`, {
        token: session.token,
        method: 'PATCH',
        body: JSON.stringify({ status })
      });
      setNotice(`Reservation ${status.toLowerCase()}.`);
      onLifecycleSuccess();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyId('');
    }
  };

  const cancelReservation = async (reservation) => {
    setBusyId(reservation.reservationId);
    setError('');
    try {
      await api(`/api/reservations/${encodeURIComponent(reservation.reservationId)}`, {
        token: session.token,
        method: 'DELETE'
      });
      setNotice('Reservation cancelled and its quantity released.');
      onLifecycleSuccess();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyId('');
    }
  };

  const processBorrow = async (reservation) => {
    setBusyId(reservation.reservationId);
    setError('');
    try {
      await api(`/api/resources/${encodeURIComponent(reservation.resourceId)}/borrow`, {
        token: session.token,
        method: 'POST',
        body: JSON.stringify({ reservationId: reservation.reservationId })
      });
      setNotice('Approved reservation checked out.');
      onLifecycleSuccess();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyId('');
    }
  };

  const processReturn = async (record) => {
    setBusyId(record.borrowRecordId);
    setError('');
    try {
      await api(`/api/resources/${encodeURIComponent(record.resourceId)}/return`, {
        token: session.token,
        method: 'POST',
        body: JSON.stringify({ borrowRecordId: record.borrowRecordId })
      });
      setNotice('Returned item and restored availability.');
      onLifecycleSuccess();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyId('');
    }
  };

  if (session?.user.role !== 'DEPARTMENT_ADMIN') {
    return <section className="panel"><h2>Department administrator</h2><p>Sign in with a department administrator account to review reservations for its assigned department.</p></section>;
  }
  return (
    <section className="panel">
      <h2>{screen === 'requests' ? `${session.user.department} Requests` : `${session.user.department} Resource Management`}</h2>
      {error && <p className="notice error" role="alert">{error}</p>}
      {notice && <p className="notice" role="status">{notice}</p>}
      {loading && <p>Loading department records…</p>}
      {screen === 'resources' && <section>
        <h3>Department resources</h3>
        <form className="search-form" onSubmit={createResource}>
          <label>Resource name<input required maxLength="160" value={resourceForm.name} onChange={(event) => setResourceForm({ ...resourceForm, name: event.target.value })} /></label>
          <label>Category<input required maxLength="100" value={resourceForm.category} onChange={(event) => setResourceForm({ ...resourceForm, category: event.target.value })} /></label>
          <label>Description<input maxLength="4000" value={resourceForm.description} onChange={(event) => setResourceForm({ ...resourceForm, description: event.target.value })} /></label>
          <label>Laboratory<input maxLength="160" value={resourceForm.laboratory} onChange={(event) => setResourceForm({ ...resourceForm, laboratory: event.target.value })} /></label>
          <label>Condition<input maxLength="80" value={resourceForm.condition} onChange={(event) => setResourceForm({ ...resourceForm, condition: event.target.value })} /></label>
          <label>Initial quantity<input type="number" min="1" step="1" required value={resourceForm.totalQuantity} onChange={(event) => setResourceForm({ ...resourceForm, totalQuantity: event.target.value })} /></label>
          <button className="button" type="submit" disabled={resourceBusy}>{resourceBusy ? 'Saving…' : 'Add resource'}</button>
        </form>
        {resources.length === 0 && !loading && !resourcesUnavailable ? <p>No resources in this department.</p> : resources.map((resource) => {
          const draft = resourceDrafts[resource.resourceId] || resource;
          const activeReservation = reservations.some((item) => (
            item.resourceId === resource.resourceId && ['PENDING', 'APPROVED'].includes(item.status)
          ));
          const hasBorrowedRecord = borrowRecords.some((record) => (
            record.resourceId === resource.resourceId && record.status === 'BORROWED'
          ));
          const deleteBlocked = activeReservation || hasBorrowedRecord;
          return (
            <article className="list-card admin-reservation" key={resource.resourceId}>
              <div>
                <strong>{resource.name}</strong>
                <span>{resource.department} · {resource.resourceId} · {resource.availableQuantity} available, {resource.reservedQuantity} reserved, {resource.borrowedQuantity} borrowed of {resource.totalQuantity}</span>
              </div>
              <label>Resource name
                <input
                  maxLength="160"
                  value={draft.name}
                  onChange={(event) => setResourceDrafts((current) => ({
                    ...current,
                    [resource.resourceId]: { ...draft, name: event.target.value }
                  }))}
                />
              </label>
              <label>Category
                <input
                  maxLength="100"
                  value={draft.category}
                  onChange={(event) => setResourceDrafts((current) => ({
                    ...current,
                    [resource.resourceId]: { ...draft, category: event.target.value }
                  }))}
                />
              </label>
              <label>Description
                <input
                  maxLength="4000"
                  value={draft.description || ''}
                  onChange={(event) => setResourceDrafts((current) => ({
                    ...current,
                    [resource.resourceId]: { ...draft, description: event.target.value }
                  }))}
                />
              </label>
              <label>Laboratory
                <input
                  maxLength="160"
                  value={draft.laboratory}
                  onChange={(event) => setResourceDrafts((current) => ({
                    ...current,
                    [resource.resourceId]: { ...draft, laboratory: event.target.value }
                  }))}
                />
              </label>
              <label>Condition
                <input
                  maxLength="80"
                  value={draft.condition || ''}
                  onChange={(event) => setResourceDrafts((current) => ({
                    ...current,
                    [resource.resourceId]: { ...draft, condition: event.target.value }
                  }))}
                />
              </label>
              <label>Available quantity
                <input
                  type="number"
                  min="0"
                  step="1"
                  value={draft.availableQuantity}
                  onChange={(event) => setResourceDrafts((current) => ({
                    ...current,
                    [resource.resourceId]: { ...draft, availableQuantity: event.target.value }
                  }))}
                />
              </label>
              <div className="stack">
                <button className="button" disabled={resourceBusy} onClick={() => updateResource(resource)}>Save resource changes</button>
                <button className="button danger" disabled={resourceBusy || deleteBlocked} onClick={() => deleteResource(resource)}>Delete resource</button>
              </div>
              {deleteBlocked && <small>Resolve active reservations and outstanding borrows before deleting this resource.</small>}
            </article>
          );
        })}
      </section>
      }
      {screen === 'requests' && <>
      <h3>Reservation requests</h3>
      {loading && <p>Loading reservation requests…</p>}
      {!loading && reservations.length === 0 ? <p>No reservation requests yet.</p> : newestFirst(reservations).map((item) => (
        <article className="list-card admin-reservation" key={item.reservationId}>
          <div><strong>{item.resourceName || item.resourceId}</strong><span>{item.department} · {item.studentId}</span></div>
          <span>Quantity: 1 · Status: {statusLabel(item.status)}</span>
          {item.createdAt && <small>Requested {formatTimestamp(item.createdAt)}</small>}
          {item.status === 'PENDING' && (
            <div className="stack">
              <button className="button" disabled={busyId === item.reservationId} onClick={() => updateStatus(item, 'APPROVED')}>Approve</button>
              <button className="button danger" disabled={busyId === item.reservationId} onClick={() => updateStatus(item, 'REJECTED')}>Reject</button>
              <button className="button secondary" disabled={busyId === item.reservationId} onClick={() => cancelReservation(item)}>Cancel request</button>
            </div>
          )}
          {item.status === 'APPROVED' && (
            <div className="stack">
              <button className="button" disabled={busyId === item.reservationId} onClick={() => processBorrow(item)}>Check out</button>
              <button className="button secondary" disabled={busyId === item.reservationId} onClick={() => cancelReservation(item)}>Cancel request</button>
            </div>
          )}
        </article>
      ))}
      <h3>Department borrow records</h3>
      {borrowRecords.filter((record) => record.status === 'BORROWED').length === 0 ? <p>No items currently checked out.</p> : borrowRecords.filter((record) => record.status === 'BORROWED').map((record) => (
        <article className="list-card admin-reservation" key={record.borrowRecordId}>
          <div><strong>{record.resourceName || record.resourceId}</strong><span>{record.department} · {record.studentId}</span></div>
          <button className="button" disabled={busyId === record.borrowRecordId} onClick={() => processReturn(record)}>Record return</button>
        </article>
      ))}
      </>}
    </section>
  );
}

function MonitorView({ nodes, nodeError, refreshNodes }) {
  const [triggering, setTriggering] = useState(false);
  const [message, setMessage] = useState('');

  const triggerElection = async () => {
    setTriggering(true);
    setMessage('');
    try {
      const result = await api('/api/distributed/election', { method: 'POST' });
      setMessage(`Election completed via ${result.triggerNode}: ${result.result?.coordinator || result.result?.latestResult?.coordinator || 'no leader reported'}.`);
      await refreshNodes();
    } catch (error) {
      setMessage(`Election could not be triggered: ${error.message}`);
    } finally {
      setTriggering(false);
    }
  };

  return (
    <section className="panel">
      <h2>Distributed node monitor</h2>
      <p>Current health view, not a formal consistent global snapshot.</p>
      {nodeError && <p className="notice error" role="alert">Unable to refresh node status: {nodeError}</p>}
      {message && <p className="notice" role="status">{message}</p>}
      <button className="button" type="button" disabled={triggering} onClick={triggerElection}>
        {triggering ? 'Running election…' : 'Trigger leader election'}
      </button>
      <ul className="node-list">
        {nodes.length ? nodes.map((node) => (
          <li key={node.department} className="node-item">
            <strong>{node.department} (priority {node.priority ?? 'unknown'})</strong>
            <span>{node.status} · {node.serviceStatus}</span>
            <span>Leader: {node.leader || 'none'}{node.leaderPriority ? ` (priority ${node.leaderPriority})` : ''}</span>
            <span>Election: {node.electionStatus || 'unknown'}</span>
            <span>Lamport: {Number.isSafeInteger(node.lamportClock) ? node.lamportClock : 'unavailable'}</span>
            <span>Last successful check / heartbeat: {node.lastHeartbeat || node.lastHealthCheck
              ? new Date(node.lastHeartbeat || node.lastHealthCheck).toLocaleString()
              : 'unavailable'}</span>
            {(node.healthError || node.leaderError) && <span role="alert">Health: {node.healthError || 'ok'} · Leader: {node.leaderError || 'ok'}</span>}
          </li>
        )) : <li>No node data available</li>}
      </ul>
    </section>
  );
}

export default App;
