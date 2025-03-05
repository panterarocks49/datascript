#!/bin/sh

set -e

(cat release-js/wrapper.prefix; cat release-js/datascript.bare.js; cat release-js/wrapper.suffix) > release-js/datascript-async.js

echo "Packed release-js/datascript-async.js"
