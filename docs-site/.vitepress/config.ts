import { existsSync } from 'node:fs'
import { defineConfig } from 'vitepress'

const sidebar = [
  {
    text: 'Get started',
    items: [
      { text: 'Introduction', link: '/introduction' },
      { text: 'Quickstart', link: '/quickstart' },
      { text: 'Use cases', link: '/use-cases' },
    ],
  },
  {
    text: 'Docs',
    items: [
      { text: 'Key concepts', link: '/concepts' },
      { text: 'Design', link: '/design' },
      {
        text: 'Governance and security',
        items: [
          { text: 'Governance', link: '/governance' },
          { text: 'Security', link: '/security' },
        ],
      },
      { text: 'Configuration', link: '/configuration' },
      { text: 'Operations', link: '/operations' },
      { text: 'Runbooks', link: '/runbooks' },
      { text: 'Plugins', link: '/plugins' },
      {
        text: 'APIs',
        items: [
          { text: 'Commands', link: '/commands' },
          { text: 'MCP tools', link: '/mcp' },
          { text: 'The wherry A2A extension', link: '/a2a-extension-v1' },
        ],
      },
    ],
  },
]

const check = (items) => {
  for (const item of items) {
    if (item.items) check(item.items)
    else if (!existsSync(new URL(`../../docs${item.link}.md`, import.meta.url))) {
      throw new Error(`Sidebar page does not exist: ${item.link}`)
    }
  }
}
check(sidebar)

// The pages are the repository's docs/, copied to src/ by the build, so they also read as plain Markdown on GitHub;
// rendered from outside this directory, their imports would not resolve against node_modules. The build also copies
// wherry-plugins' examples/ to src/examples/ for the Plugins page's snippet imports; they are not pages.
export default defineConfig({
  title: 'wherry',
  description: 'An execution fabric for delegated agent work.',
  base: '/wherry/',
  srcDir: 'src',
  srcExclude: ['examples/**'],
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
      { text: 'Quickstart', link: '/quickstart' },
      { text: 'Configuration', link: '/configuration' },
      { text: 'Commands', link: '/commands' },
      { text: 'GitHub', link: 'https://github.com/benemon/wherry' },
    ],
    sidebar,
  },
})
