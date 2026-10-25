// Fails when a link's #anchor names no id on its target page. VitePress checks only that a linked page exists, so
// this reads the ids of the built pages in .vitepress/dist and checks every link of the source pages in
// src/, the home page's feature links, and README.md's links into the published site.
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join, posix } from 'node:path'

const dist = new URL('.vitepress/dist/', import.meta.url).pathname
const src = new URL('src/', import.meta.url).pathname
const readme = new URL('../README.md', import.meta.url).pathname
const site = 'https://benemon.github.io/wherry/'

const ids = new Map()
const idsOf = (page) => {
  if (!ids.has(page)) {
    const file = join(dist, `${page}.html`)
    const html = existsSync(file) ? readFileSync(file, 'utf8') : null
    ids.set(page, html && new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1])))
  }
  return ids.get(page)
}

// Link targets outside fenced code and code spans, with the frontmatter's link: values.
const targets = (text) => {
  const out = []
  let fence = null
  for (const line of text.split('\n')) {
    const marker = line.match(/^\s*(`{3,}|~{3,})/)
    if (marker) {
      if (!fence) fence = marker[1][0]
      else if (marker[1][0] === fence) fence = null
      continue
    }
    if (fence) continue
    const prose = line.replace(/`[^`]*`/g, '')
    for (const m of prose.matchAll(/\]\(([^)\s]+)\)/g)) out.push(m[1])
    const link = prose.match(/^\s+link:\s*(\S+)\s*$/)
    if (link) out.push(link[1])
  }
  return out
}

const failures = []
const check = (from, page, target) => {
  let [path, anchor] = target.split('#')
  // VitePress checks the pages of internal links; this checks the pages of links to the published site.
  if (path.startsWith(site)) path = '/' + path.slice(site.length)
  else if (/^[a-z]+:/.test(path) || anchor === undefined) return
  const name = path === '' ? page : posix.join(path.startsWith('/') ? '/' : posix.dirname(`/${page}`), path)
    .replace(/^\//, '').replace(/\/$/, '/index').replace(/\.(md|html)$/, '') || 'index'
  const pageIds = idsOf(name)
  if (!pageIds) failures.push(`${from}: ${target}: no page ${name}`)
  else if (anchor !== undefined && !pageIds.has(decodeURIComponent(anchor))) {
    failures.push(`${from}: ${target}: no anchor #${anchor} on ${name}`)
  }
}

for (const file of readdirSync(src).filter((f) => f.endsWith('.md'))) {
  for (const target of targets(readFileSync(join(src, file), 'utf8'))) check(`docs/${file}`, file.slice(0, -3), target)
}
for (const target of targets(readFileSync(readme, 'utf8'))) if (target.startsWith(site)) check('README.md', 'index', target)

if (failures.length) {
  console.error(`Links to missing anchors:\n${failures.join('\n')}`)
  process.exit(1)
}
console.log('every link anchor exists')
