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

import { getEnhancedMonacoEnvironment } from "monaco-languageclient/vscodeApiWrapper";

/**
 * Wires `MonacoEnvironment.getWorker` to the codingame-shipped worker entries.
 *
 * Each `new Worker(new URL(...))` literal lets webpack 5 treat the URL as a
 * worker entry point and bundle the worker's transitive deps into a dedicated
 * chunk — `configureDefaultWorkerFactory` from monaco-vscode-api can't do this
 * because it only emits the upstream worker.js as a static asset (which 404s
 * on its first relative import at runtime).
 *
 * This file gets fileReplacements'd to `codingame-worker-factory.stub.ts` for
 * the test pipeline. esbuild (used by @angular/build:unit-test) resolves
 * `new URL(spec, import.meta.url)` literally relative to the source file and
 * would otherwise fail on the codingame package paths.
 */
export function registerCodingameWorkers(): void {
  const env = getEnhancedMonacoEnvironment();
  env.getWorker = (_workerId: string, label: string): Worker => {
    switch (label) {
      case "editorWorkerService":
        return new Worker(
          new URL("@codingame/monaco-vscode-editor-api/esm/vs/editor/editor.worker.js", import.meta.url),
          { type: "module" }
        );
      case "extensionHostWorkerMain":
        return new Worker(
          new URL("@codingame/monaco-vscode-api/workers/extensionHost.worker", import.meta.url),
          { type: "module" }
        );
      case "TextMateWorker":
        return new Worker(
          new URL("@codingame/monaco-vscode-textmate-service-override/worker", import.meta.url),
          { type: "module" }
        );
      default:
        throw new Error(`No worker configured for label: ${label}`);
    }
  };
}
