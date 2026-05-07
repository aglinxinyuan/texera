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
 * jsdom doesn't implement the SVG geometry APIs (`SVGSVGElement#createSVGMatrix`,
 * `createSVGPoint`, `createSVGTransform`, `getScreenCTM`, `getCTM`,
 * `getBBox`). jointjs reaches into these during graph layout and crashes
 * the spec build with `TypeError: svgDocument.createSVGMatrix is not a
 * function` etc.
 *
 * The stubs below return identity-ish geometry: matrices/points behave like
 * the identity, bounding boxes report zero dimensions. That's enough for
 * jointjs construction code to not throw; specs that actually depend on
 * accurate geometry should run under Vitest browser mode rather than
 * jsdom (tracked in #4861), but the bulk of the texera specs only need
 * jointjs to instantiate cleanly.
 */

/**
 * Register a Node ESM loader hook so every transitive `.css` import resolves
 * to an empty module. Required because the Angular `@angular/build:unit-test`
 * builder pre-bundles spec files with `externalPackages: true`, which means
 * imports like `monaco-languageclient` reach Node's native ESM loader instead
 * of Vite's transform pipeline. Without the hook, every spec that transitively
 * loads the codingame v25 stack crashes with `Unknown file extension ".css"`.
 *
 * Done at the very top of this file so the registration happens before any
 * spec body imports the affected packages. `module.register` requires Node
 * 20.6+; the project already mandates Node >= 24.
 */
import { register as registerLoader } from "node:module";
import { pathToFileURL } from "node:url";
import * as nodePath from "node:path";

registerLoader(pathToFileURL(nodePath.join(__dirname, "jsdom-css-loader-hook.mjs")));

type AnyFn = (...args: unknown[]) => unknown;

function fakeMatrix() {
  // Minimal SVGMatrix shape — just the methods jointjs touches.
  const m: Record<string, unknown> = { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0 };
  m.multiply = () => fakeMatrix();
  m.inverse = () => fakeMatrix();
  m.translate = () => fakeMatrix();
  m.scale = () => fakeMatrix();
  m.scaleNonUniform = () => fakeMatrix();
  m.rotate = () => fakeMatrix();
  m.rotateFromVector = () => fakeMatrix();
  m.flipX = () => fakeMatrix();
  m.flipY = () => fakeMatrix();
  m.skewX = () => fakeMatrix();
  m.skewY = () => fakeMatrix();
  return m;
}

function fakePoint() {
  const p: Record<string, unknown> = { x: 0, y: 0 };
  p.matrixTransform = () => fakePoint();
  return p;
}

function fakeTransform() {
  return {
    type: 0,
    matrix: fakeMatrix(),
    angle: 0,
    setMatrix: () => undefined,
    setTranslate: () => undefined,
    setScale: () => undefined,
    setRotate: () => undefined,
    setSkewX: () => undefined,
    setSkewY: () => undefined,
  };
}

function fakeRect() {
  return { x: 0, y: 0, width: 0, height: 0 };
}

const SVG_GLOBAL = (globalThis as unknown as { SVGSVGElement?: { prototype: Record<string, AnyFn> } }).SVGSVGElement;
const SVG_ELEMENT_GLOBAL = (globalThis as unknown as { SVGGraphicsElement?: { prototype: Record<string, AnyFn> } })
  .SVGGraphicsElement;

if (SVG_GLOBAL?.prototype) {
  const proto = SVG_GLOBAL.prototype;
  if (typeof proto.createSVGMatrix !== "function") proto.createSVGMatrix = fakeMatrix as AnyFn;
  if (typeof proto.createSVGPoint !== "function") proto.createSVGPoint = fakePoint as AnyFn;
  if (typeof proto.createSVGTransform !== "function") proto.createSVGTransform = fakeTransform as AnyFn;
  if (typeof proto.createSVGTransformFromMatrix !== "function")
    proto.createSVGTransformFromMatrix = fakeTransform as AnyFn;
}

if (SVG_ELEMENT_GLOBAL?.prototype) {
  const proto = SVG_ELEMENT_GLOBAL.prototype;
  if (typeof proto.getScreenCTM !== "function") proto.getScreenCTM = fakeMatrix as AnyFn;
  if (typeof proto.getCTM !== "function") proto.getCTM = fakeMatrix as AnyFn;
  if (typeof proto.getBBox !== "function") proto.getBBox = fakeRect as AnyFn;
}

/**
 * jsdom doesn't implement the Constructable Stylesheets API
 * (`new CSSStyleSheet().replaceSync(...)`), which @codingame/monaco-vscode-api
 * v25's `css.js` runtime calls when registering CSS at module load time.
 * Without it, every spec that transitively imports monaco-languageclient
 * crashes at construction.
 *
 * Stub `CSSStyleSheet` with an inert constructor whose `replaceSync` is a
 * no-op. Specs don't visually render anything, so swallowing CSS is safe.
 */
const CSS_GLOBAL = (globalThis as unknown as { CSSStyleSheet?: { prototype: Record<string, AnyFn> } }).CSSStyleSheet;
if (!CSS_GLOBAL) {
  class InertCSSStyleSheet {
    cssRules: unknown[] = [];
    replaceSync(): void {}
    replace(): Promise<void> {
      return Promise.resolve();
    }
    insertRule(): number {
      return 0;
    }
    deleteRule(): void {}
  }
  (globalThis as unknown as { CSSStyleSheet: typeof InertCSSStyleSheet }).CSSStyleSheet = InertCSSStyleSheet;
} else if (typeof CSS_GLOBAL.prototype.replaceSync !== "function") {
  CSS_GLOBAL.prototype.replaceSync = (() => undefined) as AnyFn;
  if (typeof CSS_GLOBAL.prototype.replace !== "function") {
    CSS_GLOBAL.prototype.replace = (() => Promise.resolve()) as AnyFn;
  }
}

/**
 * jsdom's Document doesn't expose `adoptedStyleSheets` (it's a Constructable
 * Stylesheets feature). The codingame runtime pushes new sheets onto it.
 */
const docProtoForCss = (globalThis as unknown as { Document?: { prototype: Record<string, unknown> } }).Document
  ?.prototype;
if (docProtoForCss && !("adoptedStyleSheets" in docProtoForCss)) {
  Object.defineProperty(docProtoForCss, "adoptedStyleSheets", {
    configurable: true,
    get() {
      return (this as { __adoptedStyleSheets?: unknown[] }).__adoptedStyleSheets ?? [];
    },
    set(v: unknown[]) {
      (this as { __adoptedStyleSheets?: unknown[] }).__adoptedStyleSheets = v;
    },
  });
}

/**
 * jsdom doesn't implement the `CSS` global namespace (`CSS.escape`,
 * `CSS.supports`). The codingame v25 theme service calls `CSS.escape(...)` to
 * sanitize icon class names. Without it, an idle-callback runner crashes the
 * worker with `TypeError: Cannot read properties of undefined (reading 'escape')`.
 *
 * Provide a minimal stub. The escape implementation mirrors the spec —
 * https://drafts.csswg.org/cssom/#serialize-an-identifier — but we only need
 * to handle the conservative case so `value === out` as often as possible
 * (otherwise a noisy `console.warn` fires every paint).
 */
const cssGlobal = globalThis as unknown as { CSS?: { escape?: (value: string) => string; supports?: AnyFn } };
if (!cssGlobal.CSS) {
  cssGlobal.CSS = {};
}
if (typeof cssGlobal.CSS.escape !== "function") {
  cssGlobal.CSS.escape = (value: string) => String(value).replace(/[!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~]/g, "\\$&");
}
if (typeof cssGlobal.CSS.supports !== "function") {
  cssGlobal.CSS.supports = (() => false) as AnyFn;
}

/**
 * jsdom doesn't implement `window.matchMedia` (the CSS media query API).
 * The codingame v25 theme service calls it during a deferred idle callback
 * to detect dark/light preference, and jsdom raises
 * `TypeError: targetWindow.matchMedia is not a function`.
 *
 * Stub with an inert MediaQueryList that always reports no match.
 */
const winForMatchMedia = globalThis as unknown as {
  matchMedia?: AnyFn;
  window?: { matchMedia?: AnyFn };
};
const matchMediaStub: AnyFn = ((query: string) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: () => undefined,
  removeListener: () => undefined,
  addEventListener: () => undefined,
  removeEventListener: () => undefined,
  dispatchEvent: () => false,
})) as AnyFn;
if (typeof winForMatchMedia.matchMedia !== "function") {
  winForMatchMedia.matchMedia = matchMediaStub;
}
if (winForMatchMedia.window && typeof winForMatchMedia.window.matchMedia !== "function") {
  winForMatchMedia.window.matchMedia = matchMediaStub;
}

/**
 * jsdom doesn't implement the legacy `document.queryCommandSupported`,
 * which monaco-editor probes during initialization. Without it the
 * editor's setup throws even when no spec actually exercises monaco.
 */
const docProto = (globalThis as unknown as { Document?: { prototype: Record<string, AnyFn> } }).Document?.prototype;
if (docProto && typeof docProto.queryCommandSupported !== "function") {
  docProto.queryCommandSupported = (() => false) as AnyFn;
}

/**
 * jsdom doesn't implement `requestIdleCallback` / `cancelIdleCallback`
 * (a Chrome-only API). Specs that pull in monaco-related modules
 * crash at construction with `ReferenceError: requestIdleCallback is
 * not defined`.
 *
 * Approximate with `setTimeout` so callbacks still fire. The deadline
 * argument is a coarse stub — enough for callers that only read
 * `didTimeout`.
 */
const idleGlobal = globalThis as unknown as Record<string, AnyFn | undefined>;
if (typeof idleGlobal.requestIdleCallback !== "function") {
  idleGlobal.requestIdleCallback = ((cb: (d: { didTimeout: boolean; timeRemaining: () => number }) => void) =>
    setTimeout(() => cb({ didTimeout: false, timeRemaining: () => 50 }), 0)) as AnyFn;
}
if (typeof idleGlobal.cancelIdleCallback !== "function") {
  idleGlobal.cancelIdleCallback = ((id: number) => clearTimeout(id)) as AnyFn;
}

/**
 * y-websocket schedules a reconnect timer the moment a service that uses
 * collaborative editing is constructed. When that timer fires AFTER vitest
 * has begun tearing down the jsdom window, jsdom's WebSocket implementation
 * crashes during construction (`Cannot read properties of null (reading
 * '_cookieJar')` → `Invalid value used as weak map key`). Vitest catches
 * this as an unhandled error and fails the run even though every test
 * passed.
 *
 * Stub WebSocket with an inert no-op so the timer can fire without
 * touching jsdom. The collaborative-editing specs that actually exercise
 * WebSocket behaviour are excluded from the test suite (component specs +
 * the workflow-action suite is the only collaboration-touching active
 * spec). Real WebSocket testing belongs under Vitest browser mode.
 */
class InertWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSING = 2;
  readonly CLOSED = 3;
  readyState = 3;
  bufferedAmount = 0;
  binaryType: "blob" | "arraybuffer" = "blob";
  url = "";
  protocol = "";
  extensions = "";
  onopen: AnyFn | null = null;
  onerror: AnyFn | null = null;
  onmessage: AnyFn | null = null;
  onclose: AnyFn | null = null;
  send(): void {}
  close(): void {}
  addEventListener(): void {}
  removeEventListener(): void {}
  dispatchEvent(): boolean {
    return false;
  }
  constructor(_url?: string, _protocols?: string | string[]) {}
}
(globalThis as unknown as { WebSocket: typeof InertWebSocket }).WebSocket = InertWebSocket;

/**
 * NgZorro's NzIconService dynamically fetches icon SVGs over HTTP from
 * `/assets/...` when the icon isn't pre-registered. jsdom's XHR
 * implementation rejects those requests with an `AggregateError`, and
 * downstream the icon lookup re-throws as `IconNotFoundError`. Vitest
 * catches both as unhandled errors, which CI treats as a hard failure
 * (locally Vitest only reports them as non-fatal warnings).
 *
 * Stubbing every spec with `NzIconModule.forChild([...])` for every
 * icon its template uses is impractical — there are dozens. Instead,
 * suppress the two specific error patterns at the process level: they
 * originate inside ngZorro's icon plumbing and don't affect the
 * assertions specs actually make.
 */
function isBenignIconError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  const stack = err instanceof Error ? err.stack ?? "" : "";
  return (
    msg.includes("[@ant-design/icons-angular]") ||
    (err instanceof Error && err.name === "AggregateError" && /xhr-utils/.test(stack)) ||
    // codingame v25 default extensions try to fetch their bundled themes /
    // language configs over `extension-file://` URIs at activation time. jsdom
    // can't resolve that scheme so the fetch rejects, but it's purely cosmetic
    // — the spec body never depends on the theme/grammar being applied.
    msg.includes("extension-file://") ||
    /workbenchThemeService|monaco-vscode-theme|monaco-vscode-.*-default-extension/.test(stack)
  );
}
process.on("uncaughtException", err => {
  if (!isBenignIconError(err)) throw err;
});
process.on("unhandledRejection", reason => {
  if (!isBenignIconError(reason)) throw reason;
});
