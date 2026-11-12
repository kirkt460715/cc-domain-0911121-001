#!/bin/sh
set -eu
npm run build
npm test
npm run acceptance
node demo_support/source.mjs --check
node demo_support/stream_demo.mjs
