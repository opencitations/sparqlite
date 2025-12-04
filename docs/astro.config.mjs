// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import rehypeExternalLinks from 'rehype-external-links';

// https://astro.build/config
export default defineConfig({
	site: 'https://opencitations.github.io',
	base: '/sparqlite/',
	markdown: {
		rehypePlugins: [
			[
				rehypeExternalLinks,
				{ target: '_blank', rel: ['noopener', 'noreferrer'] }
			]
		],
	},
	integrations: [
		starlight({
			title: 'sparqlite',
			description: 'A modern, lightweight SPARQL 1.1 client for Python',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/opencitations/sparqlite' },
			],
			sidebar: [
				{
					label: 'Getting started',
					items: [
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quick start', slug: 'getting-started/quick-start' },
					],
				},
				{
					label: 'Guides',
					items: [
						{ label: 'SELECT queries', slug: 'guides/select-queries' },
						{ label: 'ASK queries', slug: 'guides/ask-queries' },
						{ label: 'CONSTRUCT and DESCRIBE', slug: 'guides/construct-describe' },
						{ label: 'UPDATE queries', slug: 'guides/update-queries' },
						{ label: 'Error handling', slug: 'guides/error-handling' },
					],
				},
				{
					label: 'Configuration',
					items: [
						{ label: 'Connection pooling', slug: 'configuration/connection-pooling' },
						{ label: 'Retry settings', slug: 'configuration/retry-settings' },
					],
				},
				{
					label: 'Architecture',
					items: [
						{ label: 'Why pycurl?', slug: 'architecture/why-pycurl' },
						{ label: 'Benchmarks', slug: 'architecture/benchmarks' },
					],
				},
			],
		}),
	],
});
