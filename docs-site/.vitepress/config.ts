import { existsSync } from 'node:fs'
import { defineConfig } from 'vitepress'

const sidebar = [
  { text: 'Quick start', items: [{ text: 'Getting started', link: '/getting-started' }] },
  { text: 'Concepts', items: [{ text: 'Concepts', link: '/concepts' }] },
  { text: 'Operations', items: [{ text: 'Operator guide', link: '/operator-guide' }] },
  { text: 'Adapters', items: [{ text: 'Adapter plugins', link: '/adapters' }] },
  {
    text: 'Reference',
    items: [
      { text: 'Reference', link: '/reference' },
      { text: 'The wherry A2A extension', link: '/a2a-extension-v1' },
    ],
  },
]

for (const group of sidebar) {
  for (const item of group.items) {
    if (!existsSync(new URL(`../../docs${item.link}.md`, import.meta.url))) {
      throw new Error(`Sidebar page does not exist: ${item.link}`)
    }
  }
}

// The pages are the repository's docs/, copied to src/ by the build, so they also read as plain Markdown on GitHub;
// rendered from outside this directory, their imports would not resolve against node_modules.
export default defineConfig({
  title: 'wherry',
  description: 'Run a coding agent’s subagent tasks on other compute, over Kafka.',
  base: '/wherry/',
  srcDir: 'src',
  ignoreDeadLinks: false,
  vite: { publicDir: '../../brand' },
  head: [
    // The base path prefixes head links; VitePress does not add it here.
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/wherry/favicon/favicon.svg' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    ['link', { rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap' }],
    ['meta', { name: 'theme-color', content: '#00290B' }],
  ],
  themeConfig: {
    logo: { light: '/svg/wherry-horizontal.svg', dark: '/svg/wherry-horizontal-cream.svg' },
    // The lockup carries the wordmark.
    siteTitle: false,
    nav: [
      { text: 'Getting started', link: '/getting-started' },
      { text: 'Reference', link: '/reference' },
      { text: 'GitHub', link: 'https://github.com/benemon/wherry' },
    ],
    sidebar,
  },
})
