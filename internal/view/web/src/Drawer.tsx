import {
  EuiBadge,
  EuiBasicTable,
  EuiButtonEmpty,
  EuiCallOut,
  EuiComboBox,
  EuiDescriptionList,
  EuiFieldSearch,
  EuiFlyout,
  EuiFlyoutBody,
  EuiFlyoutHeader,
  EuiFormRow,
  EuiHealth,
  EuiLink,
  EuiSelect,
  EuiText,
  EuiTitle,
} from '@elastic/eui';
import type { Criteria, EuiBasicTableColumn } from '@elastic/eui';
import type { Dispatch, ReactNode, SetStateAction } from 'react';
import { useState } from 'react';
import type { View } from './App';
import { RunDrawer } from './RunDrawer';
import { abs, bytes, grouped, healthOf, rel, signedBy, stateColor, stateOrder, stateWord } from './format';
import type { FilterKey, Filters } from './model';
import { activeFilters, byClassOf, byStateOf, filterKeys, filterValue, lagText, matches, none, poolHealth, receiverHealth } from './model';
import type { Offer, Receiver, RunSummary } from './types';

interface DrawerProps {
  view: View;
  fromList: boolean;
  setFromList: (v: boolean) => void;
  setFilters: Dispatch<SetStateAction<Filters>>;
  wide: boolean;
  setWide: (v: boolean) => void;
}

// Drawer is the side panel every screen but the map is: a push flyout beside the map, or an overlay when the run
// drawer is widened.
export function Drawer(props: DrawerProps) {
  const { view, wide } = props;
  const sel = view.sel!;
  const isWide = wide && sel.k === 'run';
  const nowTag = view.scrubbing ? ' · now' : '';
  let title = '';
  let body: ReactNode = null;
  switch (sel.k) {
    case 'list':
      title = 'Runs';
      body = <RunList {...props} />;
      break;
    case 'pool':
      title = 'Pool ' + sel.id;
      body = <PoolDrawer view={view} name={sel.id} nowTag={nowTag} />;
      break;
    case 'recv':
      title = 'Receiver ' + sel.id;
      body = <ReceiverDrawer view={view} name={sel.id} nowTag={nowTag} />;
      break;
    case 'host':
      title = sel.id ? 'Host ' + sel.id : 'Unknown host';
      body = <HostDrawer view={view} name={sel.id} />;
      break;
    case 'broker':
      title = 'Broker';
      body = <BrokerDrawer view={view} />;
      break;
    case 'store':
      title = 'Storage';
      body = <StorageDrawer view={view} id={sel.id} />;
      break;
    case 'run':
      title = 'Run';
      body = <RunDrawer {...props} id={sel.id} />;
      break;
  }
  return (
    <EuiFlyout key={isWide ? 'wide' : 'push'} onClose={view.close} size={isWide ? 'l' : 480} type={isWide ? 'overlay' : 'push'} ownFocus={isWide} paddingSize="m" aria-labelledby="w-drawer-title">
      <EuiFlyoutHeader hasBorder>
        <EuiTitle size="s">
          <h2 id="w-drawer-title">{title}</h2>
        </EuiTitle>
      </EuiFlyoutHeader>
      <EuiFlyoutBody>
        <div className="w-drawer">
          {view.scrubbing && (
            <EuiCallOut title={'Showing ' + abs(new Date(view.t).toISOString())} color="warning" iconType="clock" size="s">
              <p>Runs, counts and recent failures are replayed for that moment. Status, capacity, custodian and lag have no history: they show now.</p>
            </EuiCallOut>
          )}
          {body}
        </div>
      </EuiFlyoutBody>
    </EuiFlyout>
  );
}

export const Section = ({ title, children }: { title: string; children: ReactNode }) => (
  <section className="w-section">
    <EuiTitle size="xxs">
      <h3>{title}</h3>
    </EuiTitle>
    {children}
  </section>
);

const stateBadges = (counts: Record<string, number>) =>
  stateOrder
    .concat(Object.keys(counts).filter((k) => !stateOrder.includes(k)))
    .filter((k) => counts[k])
    .map((k) => (
      <EuiBadge key={k} color={stateColor(k)}>
        {counts[k] + ' ' + stateWord(k)}
      </EuiBadge>
    ));

const classBadges = (counts: Record<string, number>) =>
  Object.keys(counts)
    .sort()
    .map((k) => (
      <EuiBadge key={k} color="hollow">
        {counts[k] + ' ' + k}
      </EuiBadge>
    ));

function RunsInWindow({ view, runs, byState, byClass, filters }: { view: View; runs: RunSummary[]; byState?: Record<string, number>; byClass?: Record<string, number>; filters: Filters }) {
  const states = byStateOf(byState, runs, view.scrubbing);
  const classes = byClassOf(byClass, runs, view.scrubbing);
  return (
    <Section title="Runs in window">
      <div className="w-row">{stateBadges(states)}</div>
      {Object.keys(classes).length > 0 && (
        <div className="w-row">
          <span className="w-subdued">Failure class</span>
          {classBadges(classes)}
        </div>
      )}
      <div>
        <EuiLink onClick={() => view.openList(filters)}>Show these runs in the run list</EuiLink>
      </div>
    </Section>
  );
}

const offersList = (offers: Offer[]) => (
  <div className="w-list">
    {offers.length === 0 && <span className="w-subdued">No offers advertised.</span>}
    {offers.map((o) => (
      <div key={o.profile} style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', fontSize: 13 }}>
        <span className="w-mono">{o.profile}</span>
        <span className="w-subdued">{(o.agent !== o.profile ? o.agent + ' · ' : '') + o.in_flight + ' of ' + o.replicas + ' in flight'}</span>
      </div>
    ))}
  </div>
);

function CapacityRing({ inFlight, capacity, size }: { inFlight: number; capacity: number; size: number }) {
  const pct = capacity ? Math.min(100, Math.round((100 * inFlight) / capacity)) : 0;
  return (
    <svg width={size} height={size} viewBox="0 0 36 36" role="img" aria-label={inFlight + ' of ' + capacity + ' in flight'}>
      <circle cx="18" cy="18" r="14" fill="none" stroke="var(--w-border-subdued)" strokeWidth="4" />
      <circle cx="18" cy="18" r="14" fill="none" stroke="var(--w-primary)" strokeWidth="4" pathLength="100" strokeDasharray={pct + ' 100'} transform="rotate(-90 18 18)" />
    </svg>
  );
}

function receiverRow(view: View, r: Receiver, nowTag: string) {
  const inFlight = view.scrubbing ? view.tally.receiverWorking[r.name] || 0 : r.in_flight;
  const health = receiverHealth(r, view.poolOf[r.pool], view.custodianOf[r.pool], view.tally);
  return (
    <button key={r.name} type="button" className="w-rowbutton" onClick={() => view.open({ k: 'recv', id: r.name })} style={{ gridTemplateColumns: '40px minmax(0, 1fr) auto', alignItems: 'center' }}>
      <CapacityRing inFlight={inFlight} capacity={r.capacity} size={36} />
      <span>
        <b style={{ fontWeight: 600, color: 'var(--w-text-heading)' }}>{r.name}</b>
        <br />
        <span className="w-subdued">{inFlight + ' of ' + r.capacity + ' in flight' + (view.scrubbing ? ' (capacity now)' : '')}</span>
      </span>
      <EuiHealth color={health.color} textSize="xs">
        {health.label + nowTag}
      </EuiHealth>
    </button>
  );
}

function PoolDrawer({ view, name, nowTag }: { view: View; name: string; nowTag: string }) {
  const p = view.poolOf[name];
  if (!p) {
    return <EuiText size="s">This pool is no longer in the model.</EuiText>;
  }
  const c = view.custodianOf[name];
  const receivers = p.receivers.map((n) => view.receiverOf[n]).filter((r): r is Receiver => !!r);
  const health = poolHealth(p, receivers, c, view.tally);
  const offers = new Map<string, Offer>();
  receivers.forEach((r) =>
    r.offers.forEach((o) => {
      const a = offers.get(o.profile) || { profile: o.profile, agent: o.agent, replicas: 0, in_flight: 0 };
      a.replicas += o.replicas;
      a.in_flight += o.in_flight;
      offers.set(o.profile, a);
    }),
  );
  const runs = view.runs.filter((r) => r.pool === name);
  return (
    <>
      <div className="w-row">
        <EuiHealth color={health.color}>{health.label}</EuiHealth>
        {p.no_terminal > 0 && <EuiBadge color="warning">{p.no_terminal + ' released without a terminal'}</EuiBadge>}
      </div>
      <EuiDescriptionList
        compressed
        type="column"
        listItems={[
          { title: 'Suspended', description: (p.suspended ? 'yes' : 'no') + nowTag },
          { title: 'Tenants', description: p.tenants.join(', ') || 'none' },
          { title: 'Storage', description: p.storage ? <span className="w-mono">{p.storage.endpoint.replace(/^https?:\/\//, '') + ' / ' + p.storage.bucket}</span> : 'none' },
          { title: 'Delivery limit', description: p.delivery_limit + ' attempts' },
          { title: 'Share group', description: (p.group.state || 'unknown') + ' · ' + p.group.members + ' members' },
          { title: 'Lag', description: <span title={abs(p.lag.at)}>{lagText(p) + nowTag}</span> },
          { title: 'In progress', description: String(view.scrubbing ? runs.filter((r) => !r.terminal).length : p.in_progress) },
        ]}
      />
      <Section title="Receivers">
        <div className="w-list">{receivers.map((r) => receiverRow(view, r, nowTag))}</div>
      </Section>
      <Section title="Custodian">
        {c ? (
          <>
            <EuiDescriptionList
              compressed
              type="column"
              listItems={[
                { title: 'Receiver', description: c.receiver },
                { title: 'Key ID', description: <span className="w-mono" style={{ fontSize: 12 }}>{c.key_id}</span> },
                { title: 'Last rotated', description: <span title={abs(c.at)}>{rel(view.now, c.at) + ' · ' + c.reason}</span> },
                { title: 'Suspended', description: (c.suspended ? 'yes' : 'no') + nowTag },
              ]}
            />
            <div className="w-row">
              <EuiBadge color="hollow" iconType="lock">
                {signedBy(c.signer)}
              </EuiBadge>
            </div>
          </>
        ) : (
          <span className="w-subdued">No custodian snapshot in the window.</span>
        )}
      </Section>
      <Section title="Offers by agent profile">{offersList([...offers.values()])}</Section>
      <RunsInWindow view={view} runs={runs} byState={p.by_state} byClass={p.by_class} filters={{ pool: name }} />
    </>
  );
}

function heldRows(view: View, held: Receiver['held'], stale: boolean) {
  return held.map((h) => (
    <button key={h.run + ':' + h.attempt} type="button" className="w-rowbutton" onClick={() => view.open({ k: 'run', id: h.run })}>
      <span className="w-mono" style={{ fontSize: 13, color: 'var(--w-text-heading)' }}>
        {h.run}
      </span>
      <EuiBadge color={stale ? 'warning' : 'primary'}>{stale ? 'stale · ' + h.stage : h.stage}</EuiBadge>
      <span className="w-subdued">attempt {h.attempt}</span>
      <span className="w-from">from {h.source}</span>
    </button>
  ));
}

function ReceiverDrawer({ view, name, nowTag }: { view: View; name: string; nowTag: string }) {
  const r = view.receiverOf[name];
  if (!r) {
    return <EuiText size="s">This receiver is no longer in the model.</EuiText>;
  }
  const c = view.custodianOf[r.pool];
  const health = receiverHealth(r, view.poolOf[r.pool], c, view.tally);
  const inFlight = view.scrubbing ? view.tally.receiverWorking[name] || 0 : r.in_flight;
  const runs = view.runs.filter((x) => x.receiver === name);
  return (
    <>
      <div className="w-row">
        <EuiHealth color={health.color}>{health.label + nowTag}</EuiHealth>
        <span className="w-subdued" title={abs(r.last_advertised)}>
          {r.last_advertised ? 'last advertised ' + rel(view.now, r.last_advertised) + nowTag : 'never advertised'}
        </span>
        {c?.receiver === name && (
          <EuiBadge color="primary" iconType="key">
            custodian
          </EuiBadge>
        )}
        {r.gated && <EuiBadge color="warning">{'gated: ' + r.gated}</EuiBadge>}
      </div>
      <div className="w-row" style={{ gap: 16, flexWrap: 'nowrap' }}>
        <CapacityRing inFlight={inFlight} capacity={r.capacity} size={64} />
        <div style={{ flex: 1 }}>
          <EuiDescriptionList
            compressed
            type="column"
            listItems={[
              { title: 'Capacity', description: inFlight + ' of ' + r.capacity + ' in flight' + (view.scrubbing ? ' (capacity now)' : '') },
              { title: 'Pool', description: <EuiLink onClick={() => view.open({ k: 'pool', id: r.pool })}>{r.pool}</EuiLink> },
              { title: 'Status', description: r.status + (r.ended ? ', ended' : '') },
            ]}
          />
        </div>
      </div>
      <Section title="Offers by agent profile">{offersList(r.offers)}</Section>
      <Section title="Holding now">
        {r.held.length === 0 && r.stale.length === 0 && !r.unidentified && <span className="w-subdued">Nothing in flight.</span>}
        {(r.held.length > 0 || r.stale.length > 0 || r.unidentified > 0) && (
          <div className="w-list">
            {heldRows(view, r.held, false)}
            {heldRows(view, r.stale, true)}
            {r.unidentified > 0 && <div className="w-subdued">{r.unidentified} more in flight, not identified</div>}
          </div>
        )}
      </Section>
      <RunsInWindow view={view} runs={runs} byState={r.by_state} byClass={r.by_class} filters={{ receiver: name }} />
    </>
  );
}

function HostDrawer({ view, name }: { view: View; name: string }) {
  const h = view.model.hosts.find((x) => x.name === name);
  const mine = view.runs.filter((r) => (name ? r.host === name : !r.host)).sort((a, b) => Date.parse(b.last) - Date.parse(a.last));
  const tenant = h ? h.tenant : mine[0]?.tenant;
  return (
    <>
      <EuiDescriptionList
        compressed
        type="column"
        listItems={[
          { title: 'Tenant', description: tenant ? <EuiLink onClick={() => view.openList({ tenant })}>{tenant}</EuiLink> : 'unknown' },
          { title: 'Status', description: h ? (h.ended ? 'ended' : 'active') : 'the run records name no host' },
        ]}
      />
      <Section title={'Runs in window (' + mine.length + ')'}>
        <div className="w-row">{stateBadges(byStateOf(h?.by_state, mine, view.scrubbing))}</div>
        {mine.length > 50 && <span className="w-subdued">Showing the newest 50 of {grouped(mine.length)} runs.</span>}
        {mine.length > 0 && (
          <div>
            <EuiLink onClick={() => view.openList({ host: name || none })}>Show these runs in the run list</EuiLink>
          </div>
        )}
        {mine.length === 0 && <span className="w-subdued">No runs from this host in the window.</span>}
        <div className="w-list">
          {mine.slice(0, 50).map((r) => (
            <button key={r.id} type="button" className="w-rowbutton" onClick={() => view.open({ k: 'run', id: r.id })}>
              <span className="w-mono" style={{ fontSize: 13, color: 'var(--w-text-heading)' }}>
                {r.id.slice(0, 16)}&hellip;
              </span>
              <EuiHealth color={healthOf(stateColor(r.state))} textSize="xs">
                {stateWord(r.state)}
              </EuiHealth>
              <span className="w-subdued">{r.pool + ' · ' + (r.receiver || 'unknown')}</span>
              <span className="w-subdued" title={abs(r.last)}>
                {rel(view.now, r.last)}
              </span>
            </button>
          ))}
        </div>
      </Section>
    </>
  );
}

function BrokerDrawer({ view }: { view: View }) {
  const m = view.model;
  return (
    <>
      {m.brokers.map((b) => (
        <div key={b.id} className="w-section">
          <EuiDescriptionList
            compressed
            type="column"
            listItems={[
              { title: 'Node', description: b.id + ' · ' + (b.controller ? 'controller' : 'broker') + (b.rack ? ' · rack ' + b.rack : '') },
              { title: 'Address', description: <span className="w-mono">{b.host + ':' + b.port}</span> },
            ]}
          />
          <Section title="Listeners">
            <div className="w-list">
              {(b.listeners || []).map((l) => (
                <span key={l} className="w-mono" style={{ fontSize: 12 }}>
                  {l}
                </span>
              ))}
              {!b.listeners?.length && <span className="w-subdued">none reported</span>}
            </div>
          </Section>
        </div>
      ))}
      <Section title={'wherry topics (' + m.topics.length + ')'}>
        <div className="w-list">
          {m.topics.map((t) => (
            <div key={t.name} style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', gap: '2px 12px' }}>
              <span className="w-mono" style={{ fontSize: 12, color: 'var(--w-text-heading)' }}>
                {t.name}
              </span>
              <span className="w-row" style={{ gap: 4 }}>
                <EuiBadge color="hollow">{t.kind}</EuiBadge>
                {t.missing && <EuiBadge color="danger">missing</EuiBadge>}
                {t.predates_log && <EuiBadge color="warning">predates the window</EuiBadge>}
                {t.error && <EuiBadge color="danger">{'error: ' + t.error}</EuiBadge>}
              </span>
              <span className="w-subdued">{t.missing ? 'not on the cluster' : 'offsets ' + t.start + ' to ' + t.end + ' · next ' + t.next}</span>
            </div>
          ))}
        </div>
      </Section>
    </>
  );
}

function StorageDrawer({ view, id }: { view: View; id: string }) {
  const s = view.model.storage.find((x) => x.endpoint + '/' + x.bucket === id);
  if (!s) {
    return <EuiText size="s">This destination is no longer in the model.</EuiText>;
  }
  return (
    <>
      {s.unreturned > 0 && <EuiCallOut title={s.unreturned + ' declared outputs were not returned'} color="warning" iconType="warning" size="s" />}
      <EuiDescriptionList
        compressed
        type="column"
        listItems={[
          { title: 'Endpoint', description: <span className="w-mono">{s.endpoint}</span> },
          { title: 'Bucket', description: <span className="w-mono">{s.bucket}</span> },
          { title: 'Pools', description: s.pools.join(', ') || 'none' },
          { title: 'Runs', description: grouped(s.runs) },
          { title: 'Outputs', description: grouped(s.outputs) },
          { title: 'Bytes', description: <span title={grouped(s.bytes) + ' bytes'}>{bytes(s.bytes)}</span> },
          { title: 'Not returned', description: String(s.unreturned) },
          { title: 'Failed runs', description: String(s.failed_runs) },
        ]}
      />
    </>
  );
}

const labelOf = (k: FilterKey, v: string) => (v === none ? 'unknown' : k === 'state' ? stateWord(v) : v);

const pageSize = 20;

// RunList is the runs of the window, newest first, filtered by state, tenant, pool, receiver, host and agent and
// searched by run ID; a row opens the run drawer.
function RunList({ view, setFromList, setFilters }: DrawerProps) {
  const f = view.filters;
  const [page, setPage] = useState(0);
  const set = (k: keyof Filters, v: string | undefined) => {
    setFilters((prev) => ({ ...prev, [k]: v || undefined }));
    setPage(0);
  };
  const listed = view.runs.filter((r) => matches(r, f)).sort((a, b) => Date.parse(b.admitted || b.first) - Date.parse(a.admitted || a.first));
  const options = (k: FilterKey) => {
    const counts: Record<string, number> = {};
    view.runs.filter((r) => matches(r, f, k)).forEach((r) => {
      const v = filterValue(r, k);
      counts[v] = (counts[v] || 0) + 1;
    });
    return Object.keys(counts)
      .sort()
      .map((v) => ({ value: v, text: labelOf(k, v) + ' (' + counts[v] + ')' }));
  };
  const columns: EuiBasicTableColumn<RunSummary>[] = [
    {
      field: 'id',
      name: 'Run',
      render: (id: string, r: RunSummary) => (
        <div>
          <div className="w-mono" style={{ fontSize: 12, color: 'var(--w-text-heading)' }}>
            {id.slice(0, 16)}&hellip;
          </div>
          <div className="w-subdued">{(r.agent || 'unknown') + ' · ' + (r.model || 'unknown')}</div>
        </div>
      ),
    },
    {
      field: 'state',
      name: 'State',
      width: '110px',
      render: (state: string, r: RunSummary) => (
        <div>
          <EuiHealth color={healthOf(stateColor(state))} textSize="xs">
            {stateWord(state)}
          </EuiHealth>
          {r.class && <div className="w-subdued">{r.class}</div>}
        </div>
      ),
    },
    {
      field: 'receiver',
      name: 'Route',
      render: (receiver: string | undefined, r: RunSummary) => (
        <div style={{ fontSize: 12, lineHeight: '16px' }}>
          <div>
            <span className="w-subdued">{r.tenant + ' · '}</span>
            <span className={r.host ? undefined : 'w-muted'}>{r.host || 'unknown'}</span>
          </div>
          <div className="w-subdued">{'→ ' + r.pool + ' · ' + (receiver || 'unknown')}</div>
        </div>
      ),
    },
    {
      field: 'last',
      name: 'Last',
      width: '80px',
      render: (last: string) => (
        <span title={abs(last)} className="w-subdued">
          {rel(view.now, last)}
        </span>
      ),
    },
  ];
  const active = activeFilters(f);
  const current = Math.min(page, Math.max(0, Math.ceil(listed.length / pageSize) - 1));
  const hostOptions = options('host').map((o) => ({ label: o.text, value: o.value }));
  return (
    <div className="w-section" style={{ gap: 12 }}>
      <EuiFieldSearch placeholder="Run ID" compressed fullWidth value={f.q || ''} onChange={(e) => set('q', e.target.value)} aria-label="Search by run ID" />
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '8px 12px' }}>
        {filterKeys
          .filter((k) => k !== 'host')
          .map((k) => (
            <EuiFormRow key={k} label={k[0].toUpperCase() + k.slice(1)} display="rowCompressed">
              <EuiSelect compressed fullWidth aria-label={k} options={[{ value: '', text: 'All' }, ...options(k)]} value={f[k] || ''} onChange={(e) => set(k, e.target.value)} />
            </EuiFormRow>
          ))}
        <EuiFormRow label="Host" display="rowCompressed">
          <EuiComboBox
            compressed
            fullWidth
            aria-label="Host"
            placeholder="All"
            singleSelection={{ asPlainText: true }}
            options={hostOptions}
            selectedOptions={hostOptions.filter((o) => o.value === f.host)}
            onChange={(selected) => set('host', selected[0]?.value)}
            isClearable
          />
        </EuiFormRow>
      </div>
      <div className="w-row" style={{ justifyContent: 'space-between' }}>
        <span className="w-subdued">
          {(listed.length === view.runs.length ? grouped(listed.length) + ' runs' : grouped(listed.length) + ' of ' + grouped(view.runs.length) + ' runs') +
            (view.scrubbing ? ', as they stood at ' + abs(new Date(view.t).toISOString()).slice(11, 16) : ', newest first')}
        </span>
        {active > 0 && (
          <EuiButtonEmpty
            size="xs"
            iconType="cross"
            onClick={() => {
              setFilters({});
              setPage(0);
            }}
          >
            Clear filters
          </EuiButtonEmpty>
        )}
      </div>
      <EuiBasicTable
        items={listed.slice(current * pageSize, current * pageSize + pageSize)}
        columns={columns}
        compressed
        tableLayout="auto"
        rowProps={(r: RunSummary) => ({
          onClick: () => {
            view.open({ k: 'run', id: r.id });
            setFromList(true);
          },
          style: { cursor: 'pointer' },
          title: r.id,
        })}
        pagination={{ pageIndex: current, pageSize, totalItemCount: listed.length, showPerPageOptions: false }}
        onChange={({ page: p }: Criteria<RunSummary>) => setPage(p?.index ?? 0)}
        noItemsMessage="No runs match these filters."
      />
    </div>
  );
}
