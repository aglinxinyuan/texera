/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Node ESM loader hook used in the Vitest worker. Intercepts every `.css`
 * import and replaces it with an empty module.
 *
 * Why this exists: the Angular `@angular/build:unit-test` builder pre-bundles
 * spec files with esbuild and `externalPackages: true`, so transitive imports
 * (e.g. `monaco-languageclient` -> `@codingame/monaco-vscode-api` -> raw
 * `aria.css`) reach Node's native ESM loader instead of Vite's transform
 * pipeline. Node has no built-in handler for `.css` and crashes the worker
 * with `TypeError: Unknown file extension ".css"`.
 *
 * Vitest's `server.deps.inline` does NOT cover this because the spec is
 * already bundled before vitest sees it. The loader hook is the only place
 * we can intervene.
 *
 * Specs don't render anything, so swallowing CSS at module load is safe.
 * Activated by the setupFile `jsdom-svg-polyfill.ts` calling
 * `module.register(...)` at module init.
 */
export function resolve(specifier, context, nextResolve) {
  if (specifier.endsWith(".css") || /\.css(\?|$)/.test(specifier)) {
    return {
      url: "data:text/javascript,export%20default%20%7B%7D%3B",
      shortCircuit: true,
      format: "module",
    };
  }
  return nextResolve(specifier, context);
}
