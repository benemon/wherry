import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import type { Plugin } from 'vite';

// The licences the bundle may carry: the Go allowlist of the Makefile plus the fonts' OFL, and the three Elastic
// packages under the Elastic License 2.0 by election. A package outside the list fails the build: investigate the
// package, do not widen the list to get green.
const allowed = new Set(['MIT', 'Apache-2.0', 'BSD-2-Clause', 'BSD-3-Clause', '0BSD', 'ISC', 'MPL-2.0', 'OFL-1.1']);
const elastic = new Set(['@elastic/eui', '@elastic/eui-theme-borealis', '@elastic/eui-theme-common']);

interface Pkg {
  name: string;
  version: string;
  license: string;
  dir: string;
}

// permitted reads an SPDX expression: every licence it names must be in the list, so a conjunction passes only when
// each part does and a disjunction is not resolved in the package's favour.
const permitted = (license: string): boolean => {
  const parts = license.replace(/[()]/g, '').split(/\s+(?:AND|OR|WITH)\s+/);
  return parts.length > 0 && parts.every((p) => allowed.has(p.trim()));
};

// packageOf is the package a bundled module id belongs to: the innermost node_modules entry on its path.
function packageOf(id: string): Pkg | undefined {
  const at = id.lastIndexOf('/node_modules/');
  if (at < 0) {
    return undefined;
  }
  const rest = id.slice(at + '/node_modules/'.length).split('/');
  const name = rest[0].startsWith('@') ? rest[0] + '/' + rest[1] : rest[0];
  const dir = id.slice(0, at + '/node_modules/'.length) + name;
  const meta = JSON.parse(readFileSync(join(dir, 'package.json'), 'utf8'));
  let license = typeof meta.license === 'string' ? meta.license : meta.license?.type ?? '';
  if (Array.isArray(meta.licenses) && meta.licenses.length) {
    license = meta.licenses.map((l: { type: string }) => l.type).join(' OR ');
  }
  return { name, version: meta.version, license, dir };
}

function textsOf(p: Pkg): string[] {
  const files = readdirSync(p.dir).filter((f) => /^(licen[cs]e|notice)/i.test(f) && statSync(join(p.dir, f)).isFile()).sort();
  const texts = files.map((f) => `--- ${f} ---\n` + readFileSync(join(p.dir, f), 'utf8').trim());
  if (elastic.has(p.name)) {
    const elv2 = join(p.dir, 'licenses', 'ELASTIC-LICENSE-2.0.md');
    if (existsSync(elv2)) {
      texts.push('--- licenses/ELASTIC-LICENSE-2.0.md ---\n' + readFileSync(elv2, 'utf8').trim());
    }
  }
  return texts;
}

// notices writes THIRD_PARTY_NOTICES beside the bundle: every package a bundled module came from, its licence and
// the licence and notice files it ships, and refuses a licence outside the list.
export function notices(): Plugin {
  return {
    name: 'wherry-notices',
    generateBundle(_, bundle) {
      // The modules rendered into the chunks, not the whole module graph: what the page ships.
      const ids = Object.values(bundle).flatMap((out) => (out.type === 'chunk' ? Object.keys(out.modules) : []));
      const packages = new Map<string, Pkg>();
      for (const id of ids) {
        if (id.startsWith('\0')) {
          continue;
        }
        const p = packageOf(id);
        if (p && !packages.has(p.name)) {
          packages.set(p.name, p);
        }
      }
      const out: string[] = [
        'Third-party notices for the wherry operator view page, bundled from the npm packages below.',
        'The Elastic packages are dual-licensed under the Server Side Public License v1 and the Elastic License 2.0;',
        'wherry takes them under the Elastic License 2.0, whose text follows each. Every other package is under the',
        'licence named, with the text it ships.',
        '',
      ];
      const bad: string[] = [];
      for (const name of [...packages.keys()].sort()) {
        const p = packages.get(name)!;
        const license = elastic.has(p.name) ? 'Elastic-2.0' : p.license;
        if (!elastic.has(p.name) && !permitted(p.license)) {
          bad.push(`${p.name}@${p.version}: ${p.license || '(none)'}`);
        }
        out.push('='.repeat(78), `${p.name} ${p.version} (${license})`, '='.repeat(78), '', ...textsOf(p), '');
      }
      if (bad.length) {
        throw new Error('bundled packages outside the licence allowlist:\n' + bad.join('\n'));
      }
      this.emitFile({ type: 'asset', fileName: 'THIRD_PARTY_NOTICES', source: out.join('\n') });
    },
  };
}
