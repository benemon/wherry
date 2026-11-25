// The placeholder page: the model as plain tables, every value rendered as text through textContent, never as
// markup; output names, status messages and transcript lines, the agent's text, go the same way. The token arrives in
// the fragment, is posted once for the session cookie and cleared from the address bar.
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

  // The transcript of the run shown: its cursor, and while the run works, a follow that asks from the cursor on each
  // model event and at most every two seconds. generation counts the runs opened, so a response that arrives after
  // another run, or the same run again, was opened is dropped rather than applied to the view now open.
  var transcript = { run: null, generation: 0, attempt: 0, first: null, next: null, prev: null, complete: false, retry: false, timer: null, pending: false, lines: [] };

  // pageSize is the lines one request asks for.
  var pageSize = 200;

  function render(model) {
    // The run shown stays across re-renders, so its transcript can follow.
    var kept = document.getElementById('detail');
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
    var detail = kept || document.createElement('section');
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
      if (run.reason) {
        detail.appendChild(keyValues('Failure reason', { class: run.class, reason: run.reason }));
      }
      run.attempts.forEach(function (attempt) {
        var title = 'Attempt ' + attempt.number + ' on ' + attempt.receiver + (attempt.transcript_chunks ? ', about ' + attempt.transcript_chunks + ' chunks' : '');
        detail.appendChild(table(title, ['at', 'kind', 'state', 'message', 'truncated', 'event_id', 'receiver_at', 'log_offset', 'event', 'class', 'signer'], attempt.events));
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
      showTranscript(run, detail);
    }).catch(function (err) { text(status, 'run: ' + err.message); });
  }

  // lineText is a line as the page shows it: its text, or a marker for bytes that are not text.
  function lineText(line) {
    if (line.oversize) { return '[oversize line of ' + line.size + ' bytes, not stored]'; }
    if (line.raw) { return '[' + line.size + ' bytes, not UTF-8]'; }
    return line.text + (line.truncated ? ' [cut at 16 KiB of ' + line.size + ']' : '');
  }

  function transcriptPath(run, query) {
    return '/api/runs/' + encodeURIComponent(run) + '/transcript?' + query.toString();
  }

  // cursorQuery is query with the cursor's attempt and first, so a change of either is answered with a reset.
  function cursorQuery(query) {
    query.set('attempt', transcript.attempt);
    if (transcript.first !== null) { query.set('first', transcript.first); }
    return query;
  }

  function renderTranscript() {
    var section = document.getElementById('transcript');
    if (!section) { return; }
    section.replaceChildren();
    var state = { attempt: transcript.attempt, first: transcript.first, lines: transcript.lines.length, complete: transcript.complete, retry: transcript.retry, predates_log: transcript.predates_log, truncated: transcript.truncated, read: transcript.read, evicted: transcript.evicted };
    section.appendChild(keyValues('Transcript', state));
    if (transcript.prev !== null) {
      var older = text(document.createElement('button'), 'Older lines');
      older.addEventListener('click', function () { olderLines(); });
      section.appendChild(older);
    }
    // Each line's attempt and seq sit in their own elements and its text in a bdi, so a bidi control or a line break in
    // the text cannot reorder or imitate a neighbouring row's prefix.
    var lines = document.createElement('div');
    lines.className = 'lines';
    transcript.lines.forEach(function (line) {
      var row = document.createElement('div');
      row.className = 'line';
      var attempt = text(document.createElement('span'), line.attempt);
      attempt.className = 'attempt';
      row.appendChild(attempt);
      var seq = text(document.createElement('span'), line.seq);
      seq.className = 'seq';
      row.appendChild(seq);
      row.appendChild(text(document.createElement('bdi'), lineText(line)));
      lines.appendChild(row);
    });
    section.appendChild(lines);
  }

  // ask requests the page query names for the run open now and applies the response through apply, only while that
  // run, at the same generation, is still the one open; whatever happened, an error included, the transcript is
  // rendered again and the follow rescheduled. One request is in flight at a time.
  function ask(query, apply) {
    var run = transcript.run, generation = transcript.generation;
    var same = function () { return transcript.run === run && transcript.generation === generation; };
    transcript.pending = true;
    return api(transcriptPath(run, query)).then(function (response) {
      // apply may ask again, a reset's reload or a gap's next page: the follow waits for that too.
      if (same()) { return apply(response); }
    }).catch(function (err) {
      if (same()) { text(status, 'transcript: ' + err.message); }
    }).then(function () {
      if (!same()) { return; }
      transcript.pending = false;
      renderTranscript();
      scheduleFollow();
    });
  }

  // merge folds fresh lines into the known ones by seq, in seq order without duplicates, a fresh line replacing a
  // known one at its seq.
  function merge(fresh) {
    var bySeq = {};
    transcript.lines.forEach(function (line) { bySeq[line.seq] = line; });
    fresh.forEach(function (line) { bySeq[line.seq] = line; });
    transcript.lines = Object.keys(bySeq).map(function (seq) { return bySeq[seq]; }).sort(function (a, b) { return a.seq - b.seq; });
  }

  // take folds a response in: a reset reloads the tail; a retry says the tail is not indexed yet, so nothing is shown
  // and the follow asks tail again; lines before or after the known ones are merged in.
  function take(response, where) {
    if (response.reset) { transcript.next = null; return loadTail(); }
    transcript.attempt = response.attempt;
    transcript.first = response.first;
    transcript.complete = response.complete;
    transcript.retry = response.retry === true;
    transcript.predates_log = response.predates_log;
    transcript.truncated = response.truncated;
    transcript.read = response.read;
    transcript.evicted = response.evicted;
    if (response.retry) {
      transcript.lines = [];
      transcript.prev = null;
      transcript.next = null;
      transcript.complete = false;
      return;
    }
    if (where === 'tail') {
      transcript.lines = response.lines;
      transcript.prev = response.prev === undefined ? null : response.prev;
      transcript.next = response.next === undefined ? null : response.next;
      return;
    }
    var first = transcript.lines.length ? transcript.lines[0].seq : null;
    merge(response.lines);
    if (where === 'before') {
      transcript.prev = response.prev === undefined ? null : response.prev;
      // A whole older page's next is the first known seq, the one asked before; a page cut short has a lower next,
      // and the lines between it and the first known one are asked from there.
      if (response.next !== undefined && first !== null && response.next < first) { return fillGap(response.next); }
      return;
    }
    transcript.next = response.next === undefined ? null : response.next;
  }

  function loadTail() {
    var query = new URLSearchParams();
    query.set('tail', pageSize);
    return ask(query, function (response) { return take(response, 'tail'); });
  }

  function olderLines() {
    if (transcript.prev === null || transcript.pending) { return; }
    var query = cursorQuery(new URLSearchParams());
    query.set('before', transcript.prev);
    query.set('limit', pageSize);
    return ask(query, function (response) { return take(response, 'before'); });
  }

  // fillGap asks the lines from seq up to the first known one at or above it, after a cut older page, merging them in
  // seq order; it asks again from the response's next while the lines returned reached no known one, and moves the
  // cursor on when the page reached past the known lines.
  function fillGap(seq) {
    var known = transcript.lines.filter(function (line) { return line.seq >= seq; });
    var query = cursorQuery(new URLSearchParams());
    query.set('from', seq);
    query.set('limit', pageSize);
    return ask(query, function (response) {
      if (response.reset) { transcript.next = null; return loadTail(); }
      var reached = response.lines.some(function (line) { return known.some(function (k) { return k.seq === line.seq; }); });
      merge(response.lines);
      if (response.next !== undefined && (transcript.next === null || response.next > transcript.next)) { transcript.next = response.next; }
      if (!reached && response.next !== undefined && known.length && response.next < known[0].seq) { return fillGap(response.next); }
    });
  }

  // follow asks from the cursor while the run is not complete, or the tail again while none is indexed; a reset
  // reloads the tail. A request in flight is left to reschedule it.
  function follow() {
    transcript.timer = null;
    if (!document.getElementById('transcript')) { transcript.run = null; }
    if (transcript.complete || transcript.pending || transcript.run === null) { return; }
    if (transcript.next === null) { return loadTail(); }
    var query = cursorQuery(new URLSearchParams());
    query.set('from', transcript.next);
    query.set('limit', '1000');
    return ask(query, function (response) { return take(response, 'from'); });
  }

  // scheduleFollow asks again within two seconds while the run is not complete.
  function scheduleFollow() {
    if (transcript.complete || transcript.timer !== null || transcript.run === null) { return; }
    transcript.timer = setTimeout(follow, 2000);
  }

  // followNow asks at once, on a model event: the timer is cleared and follow runs, or waits for the request in flight
  // to reschedule it.
  function followNow() {
    if (transcript.run === null || transcript.complete) { return; }
    if (transcript.timer !== null) { clearTimeout(transcript.timer); transcript.timer = null; }
    follow();
  }

  function showTranscript(run, detail) {
    if (transcript.timer !== null) { clearTimeout(transcript.timer); }
    transcript = { run: run.id, generation: transcript.generation + 1, attempt: 0, first: null, next: null, prev: null, complete: false, retry: false, timer: null, pending: false, lines: [] };
    var section = document.createElement('section');
    section.id = 'transcript';
    detail.appendChild(section);
    loadTail();
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
    source.addEventListener('model', function () {
      load();
      followNow();
    });
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
