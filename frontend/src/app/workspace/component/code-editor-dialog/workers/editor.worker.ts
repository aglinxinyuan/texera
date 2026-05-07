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

// Worker entry — referenced via `new Worker(new URL('./editor.worker', import.meta.url))`
// from the editor component. Re-exporting the codingame-shipped worker as a local
// entry forces webpack to bundle the worker's transitive deps into the chunk.
// Without this trampoline, webpack only emits the upstream `worker.js` (a single
// `import './vscode/...'` line) and the browser fails to resolve the relative path
// at runtime.
import "@codingame/monaco-vscode-editor-api/esm/vs/editor/editor.worker.js";
