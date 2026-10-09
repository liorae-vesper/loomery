// SPDX-License-Identifier: MPL-2.0

// The published documentation site. Every page in `docs/` is listed in the sidebar
// below; a page that is not listed is reported by `mise run docs-links`, which also
// checks that no page here links into `workpad/` (staged notes are expected to
// disappear, so a published link to one would rot).
//
// Mermaid comes from `vitepress-plugin-mermaid`, which renders the ```mermaid fences
// these pages already use and follows the light/dark theme. `mise run docs-mermaid`
// parses the same diagrams in CI, so a broken diagram fails before a build does.

import { withMermaid } from 'vitepress-plugin-mermaid'

export default withMermaid({
  title: 'Loomery',
  description: 'An event-sourced backend for team collaboration, built in Rust.',
  lang: 'en',
  lastUpdated: true,
  // A dead link fails the build rather than rendering as a silent 404.
  ignoreDeadLinks: false,

  themeConfig: {
    nav: [
      { text: 'Overview', link: '/architecture' },
      { text: 'Reference', link: '/configuration' },
      { text: 'Tutorials', link: '/tutorials/shell-group' },
    ],

    sidebar: [
      {
        text: 'Overview',
        items: [
          { text: 'Architecture at a glance', link: '/architecture' },
          { text: 'Domain model', link: '/domain-model' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'Configuration', link: '/configuration' },
          { text: 'Raft configuration', link: '/raft-configuration' },
          { text: 'Runtime host', link: '/host' },
          { text: 'Shell', link: '/shell' },
          { text: 'Control plane', link: '/control-plane' },
          { text: 'Gateway', link: '/gateway' },
          { text: 'Outbox and sagas', link: '/outbox-and-sagas' },
          { text: 'Search', link: '/search' },
          { text: 'Storage layout', link: '/storage-layout' },
        ],
      },
      {
        text: 'Operating',
        items: [
          { text: 'Test services', link: '/testing-services' },
          { text: 'Third-party licenses', link: '/third-party-licenses' },
        ],
      },
      {
        text: 'Tutorials',
        items: [
          { text: 'Group port', link: '/tutorials/shell-group' },
          { text: 'Genesis worker', link: '/tutorials/genesis-worker' },
          { text: 'OpenRaft spike', link: '/tutorials/openraft-spike' },
        ],
      },
    ],

    outline: { level: [2, 3] },
    search: { provider: 'local' },
    mermaid: {},
  },
})
