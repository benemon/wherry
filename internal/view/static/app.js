// The placeholder page: the model as plain tables, every value rendered as text through textContent, never as
// markup; output names, the one tenant-chosen text shown, go the same way. The token arrives in the fragment, is posted once for the session cookie and cleared from the address bar.
(function () {
  'use strict';
  var main = document.getElementById('main');
  var status = document.getElementById('status');

  function text(node, value) {
    node.textContent = value === undefined || value === null ? '' : String(value);
    return node;
  }

  function cell(tag, value) {
    var node = document.createElement(tag);
    if (value !== null && typeof value === 'object') {
      text(node, JSON.stringify(value));
    } else {
      text(node, value);
    }
    return node;
  }

  function table(title, columns, rows, onRow) {
    var section = document.createElement('section');
    section.appendChild(text(document.createElement('h2'), title + ' (' + rows.length + ')'));
    var t = document.createElement('table');
    var head = document.createElement('tr');
    columns.forEach(function (column) { head.appendChild(cell('th', column)); });
    t.appendChild(head);
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      columns.forEach(function (column) { tr.appendChild(cell('td', row[column])); });
      if (onRow) {
        tr.className = 'run';
        tr.addEventListener('click', function () { onRow(row); });
      }
      t.appendChild(tr);
    });
    section.appendChild(t);
    return section;
  }

  function keyValues(title, object) {
    var rows = Object.keys(object || {}).sort().map(function (key) { return { key: key, value: object[key] }; });
    return table(title, ['key', 'value'], rows);
  }

  function api(path, options) {
    return fetch(path, Object.assign({ credentials: 'same-origin' }, options || {})).then(function (response) {
      if (!response.ok) { throw new Error('HTTP ' + response.status); }
      return response.status === 204 ? null : response.json();
    });
  }

  function render(model) {
    main.replaceChildren();
    text(status, 'version ' + model.version + ', at ' + model.now + ', window from ' + model.window_start + (model.rebuilt ? '' : ', rebuilding'));
    main.appendChild(keyValues('Counts', model.counts));
    main.appendChild(table('Bounds hit', ['name', 'hits'], model.bounds || []));
    if (model.errors && model.errors.length) {
      main.appendChild(table('Recent errors', ['error'], model.errors.map(function (e) { return { error: e }; })));
    }
    main.appendChild(table('Brokers', ['id', 'host', 'port', 'rack', 'controller', 'listeners'], model.brokers));
    main.appendChild(table('Pools', ['name', 'suspended', 'receivers', 'tenants', 'custodian', 'storage', 'delivery_limit', 'group', 'lag', 'runs', 'in_progress', 'by_state', 'by_class', 'no_terminal', 'agents'], model.pools));
    main.appendChild(table('Receivers', ['name', 'pool', 'status', 'ended', 'capacity', 'in_flight', 'gated', 'offers', 'last_advertised', 'profiles', 'runs', 'by_state', 'by_class', 'held', 'stale', 'unidentified'], model.receivers));
    main.appendChild(table('Custodians', ['pool', 'receiver', 'status', 'key_id', 'reason', 'at', 'suspended', 'signer'], model.custodians));
    main.appendChild(table('Hosts', ['tenant', 'name', 'ended', 'runs', 'by_state'], model.hosts));
    main.appendChild(table('Tenants', ['name', 'ended', 'pools', 'hosts', 'runs'], model.tenants));
    main.appendChild(table('S3 destinations', ['endpoint', 'bucket', 'pools', 'runs', 'outputs', 'bytes', 'unreturned', 'failed_runs'], model.storage));
    main.appendChild(table('Topics', ['name', 'kind', 'next', 'start', 'end', 'missing', 'predates_log', 'error'], model.topics));
    var runs = table('Runs' + (model.truncated ? ', truncated of ' + model.runs_total : ''), ['id', 'tenant', 'host', 'pool', 'receiver', 'agent', 'attempt', 'stage', 'state', 'class', 'storage', 'cancelled', 'admitted', 'last', 'outputs'], model.runs, showRun);
    main.appendChild(runs);
    var detail = document.createElement('section');
    detail.id = 'detail';
    main.appendChild(detail);
  }

  function showRun(row) {
    api('/api/runs/' + encodeURIComponent(row.id)).then(function (run) {
      var detail = document.getElementById('detail');
      detail.replaceChildren();
      detail.appendChild(text(document.createElement('h2'), 'Run ' + run.id));
      detail.appendChild(table('Path', ['at', 'step', 'actor', 'attempt', 'detail'], run.path));
      detail.appendChild(table('Echoes', ['receiver', 'host', 'pool', 'agent', 'model', 'at', 'signer'], run.echoes));
      run.attempts.forEach(function (attempt) {
        detail.appendChild(table('Attempt ' + attempt.number + ' on ' + attempt.receiver, ['at', 'kind', 'state', 'event_id', 'receiver_at', 'log_offset', 'event', 'class', 'signer'], attempt.events));
      });
      detail.appendChild(table('Open exchange', ['attempt', 'receiver', 'at', 'signer', 'answer', 'custodian', 'reason', 'answered_at', 'answer_signer'], run.opens));
      detail.appendChild(table('Cancels', ['host', 'at', 'signer'], run.cancels));
      if (run.result) {
        detail.appendChild(keyValues('Result', { receiver: run.result.receiver, attempt: run.result.attempt, state: run.result.state, at: run.result.at, profile: run.result.profile, reply_size: run.result.reply_size, reply_cut: run.result.reply_cut, artifacts: run.result.artifacts, signer: run.result.signer }));
        detail.appendChild(table('Outputs', ['name', 'size', 'sha256'], run.result.outputs));
        detail.appendChild(table('S3 outputs', ['name', 'size', 'sha256'], run.result.s3_outputs));
        detail.appendChild(table('Not returned', ['name', 'size', 'reason'], run.result.not_returned));
      }
      detail.appendChild(keyValues('Storage', run.storage || { unknown: run.storage_unknown }));
      var button = text(document.createElement('button'), 'Load spans');
      button.addEventListener('click', function () { showTrace(run.id, detail); });
      detail.appendChild(button);
    }).catch(function (err) { text(status, 'run: ' + err.message); });
  }

  function flatten(nodes, depth, rows) {
    nodes.forEach(function (node) {
      rows.push({ depth: depth, name: node.name, kind: node.kind, service: node.service, start: node.start, duration_ms: node.duration_ms, status: node.status, attributes: node.attributes, marker: node.marker, links: node.links });
      flatten(node.children || [], depth + 1, rows);
    });
    return rows;
  }

  function showTrace(id, detail) {
    api('/api/runs/' + encodeURIComponent(id) + '/trace').then(function (forest) {
      var old = document.getElementById('trace');
      if (old) { old.remove(); }
      var section = document.createElement('section');
      section.id = 'trace';
      section.appendChild(keyValues('Spans (advisory timing)', { spans: forest.spans, traces: forest.traces.length, markers: forest.markers, read: forest.read, next: forest.next, complete: forest.complete }));
      forest.traces.forEach(function (trace) {
        section.appendChild(table('Trace ' + trace.id, ['depth', 'name', 'kind', 'service', 'start', 'duration_ms', 'status', 'attributes', 'marker', 'links'], flatten(trace.roots, 0, [])));
      });
      detail.appendChild(section);
    }).catch(function (err) { text(status, 'spans: ' + err.message); });
  }

  function load() {
    return api('/api/model').then(render).catch(function (err) { text(status, 'model: ' + err.message); });
  }

  function stream() {
    var source = new EventSource('/api/stream');
    source.addEventListener('model', function () { load(); });
    source.onerror = function () { text(status, 'stream closed; reload the page'); };
  }

  function start() {
    var match = /^#t=([0-9a-f]+)$/.exec(window.location.hash);
    if (match) {
      history.replaceState(null, '', window.location.pathname);
      var body = new URLSearchParams();
      body.set('token', match[1]);
      return api('/session', { method: 'POST', body: body }).then(function () { load(); stream(); }).catch(function (err) { text(status, 'session: ' + err.message); });
    }
    load().then(stream);
  }

  start();
})();
