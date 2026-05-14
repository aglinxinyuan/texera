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

import { Injectable } from "@angular/core";
import * as joint from "jointjs";
import { Subscription } from "rxjs";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { ExecutionState, isWebDataUpdate, isWebPaginationUpdate } from "../../types/execute-workflow.interface";
import { IndexableObject } from "../../types/result-table.interface";

const SVG_NS = "http://www.w3.org/2000/svg";

interface ActiveBubble {
  el: SVGGElement;
  start: number;
  duration: number;
  pathEl: SVGPathElement;
  pathLen: number;
  linkID: string;
  sourceOperatorID: string;
  contentKey: string; // "" for dot; otherwise the rendered text — used to skip redundant updates
}

/**
 * Animates "tuple bubbles" along workflow edges while a workflow is running.
 *
 * Visual modes:
 *   - dot   (no field cached yet): a small filled circle
 *   - bare  (single field):        plain text, no background pill
 *   - pill  (2+ fields):           rounded-rect pill with comma-separated values
 *
 * Trigger: per-tick output-count delta from OperatorStatisticsUpdateEvent.
 * Content: latest tuple cached from WebResultUpdateEvent (set mode),
 *          PaginatedResultEvent, or OperatorCurrentTuplesUpdateEvent (pause).
 *          When content arrives mid-flight, active dot bubbles are upgraded
 *          in place.
 */
@Injectable({ providedIn: "root" })
export class EdgeTupleAnimationService {
  private static readonly MAX_INFLIGHT_PER_LINK = 4;
  private static readonly MAX_SPAWN_PER_TICK = 5;
  private static readonly BUBBLE_DURATION_MS = 1400;
  private static readonly STAGGER_MS = 140;
  private static readonly MAX_TEXT_LEN = 28;
  private static readonly MAX_FIELDS_IN_BUBBLE = 3;
  private static readonly PAG_REQUEST_INTERVAL_MS = 600;

  private paper: joint.dia.Paper | null = null;
  private overlayLayer: SVGGElement | null = null;
  private enabled = true;

  private latestFieldsByOperator = new Map<string, string[]>();
  private prevOutputCountByOperator = new Map<string, number>();
  private inflightCountByLink = new Map<string, number>();
  private lastPagRequestAt = new Map<string, number>();
  private active: ActiveBubble[] = [];
  private rafHandle: number | null = null;
  private sendPatched = false;

  private subscriptions = new Subscription();

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
    private workflowActionService: WorkflowActionService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {}

  public setEnabled(on: boolean): void {
    this.enabled = on;
    if (!on) this.clearAllBubbles();
  }

  public isEnabled(): boolean {
    return this.enabled;
  }

  public attachToPaper(paper: joint.dia.Paper): void {
    if (this.paper === paper && this.overlayLayer && this.overlayLayer.isConnected) {
      return;
    }
    this.clearAllBubbles();
    this.paper = paper;
    this.overlayLayer = this.createOverlayLayer(paper);
    this.patchExecuteRequestOnce();
    this.subscribe();
  }

  /**
   * Inject all operator IDs into opsToViewResult on outgoing WorkflowExecuteRequest.
   * Forces the backend to materialize results for every operator, so
   * WebResultUpdateEvent fires for every op and the content cache stays warm.
   * Idempotent — patches the websocket service's send method once.
   */
  private patchExecuteRequestOnce(): void {
    if (this.sendPatched) return;
    this.sendPatched = true;
    const svc = this.workflowWebsocketService;
    const original = svc.send.bind(svc);
    svc.send = (<T extends keyof any>(type: T, payload: any): void => {
      if (type === "WorkflowExecuteRequest" && payload?.logicalPlan?.operators) {
        const allIds: string[] = payload.logicalPlan.operators.map((o: { operatorID: string }) => o.operatorID);
        const merged = new Set<string>(payload.logicalPlan.opsToViewResult ?? []);
        for (const id of allIds) merged.add(id);
        payload.logicalPlan.opsToViewResult = Array.from(merged);
      }
      return original(type as any, payload);
    }) as typeof svc.send;
  }

  private createOverlayLayer(paper: joint.dia.Paper): SVGGElement {
    const svg = paper.svg as SVGSVGElement;
    const cellsLayer = svg.querySelector(".joint-cells-layer") as SVGGElement | null;
    const host: SVGGElement | SVGSVGElement = cellsLayer ?? svg;
    const layer = document.createElementNS(SVG_NS, "g") as SVGGElement;
    layer.setAttribute("class", "tuple-animation-layer");
    layer.style.pointerEvents = "none";
    host.appendChild(layer);
    return layer;
  }

  private subscribe(): void {
    this.subscriptions.unsubscribe();
    this.subscriptions = new Subscription();

    // Content cache: WebResultUpdateEvent
    // - WebDataUpdate (set/delta mode): inline table → cache directly.
    // - WebPaginationUpdate (typical pipeline ops): no inline table → fire a paginated fetch.
    this.subscriptions.add(
      this.workflowWebsocketService.subscribeToEvent("WebResultUpdateEvent").subscribe(evt => {
        const updates = (evt as any).updates as Record<string, unknown> | undefined;
        if (!updates) return;
        for (const opId of Object.keys(updates)) {
          const u = updates[opId];
          if (!u) continue;
          if (isWebDataUpdate(u as any) && Array.isArray((u as any).table) && (u as any).table.length > 0) {
            const table = (u as any).table as IndexableObject[];
            this.cacheFieldsForOp(opId, this.fieldsFromRow(table[table.length - 1]));
          } else if (isWebPaginationUpdate(u as any) && (u as any).totalNumTuples > 0) {
            this.requestLatestTupleFor(opId, (u as any).totalNumTuples as number);
          }
        }
      })
    );

    // Content cache: PaginatedResultEvent
    this.subscriptions.add(
      this.workflowWebsocketService.subscribeToEvent("PaginatedResultEvent").subscribe(evt => {
        if (evt.table && evt.table.length > 0) {
          this.cacheFieldsForOp(evt.operatorID, this.fieldsFromRow(evt.table[evt.table.length - 1] as IndexableObject));
        }
      })
    );

    // Content cache: pause-time current tuples
    this.subscriptions.add(
      this.workflowWebsocketService.subscribeToEvent("OperatorCurrentTuplesUpdateEvent").subscribe(evt => {
        const sample = evt.tuples?.[0]?.tuple;
        if (sample && sample.length > 0) {
          this.cacheFieldsForOp(evt.operatorID, this.fieldsFromArray(sample));
        }
      })
    );

    // Timing trigger: per-operator output count delta
    this.subscriptions.add(
      this.workflowStatusService.getStatusUpdateStream().subscribe(stats => {
        if (!this.enabled || !this.paper) return;
        for (const opId of Object.keys(stats)) {
          const current = stats[opId].aggregatedOutputRowCount ?? 0;
          const prev = this.prevOutputCountByOperator.get(opId) ?? 0;
          const delta = current - prev;
          this.prevOutputCountByOperator.set(opId, current);
          if (delta <= 0) continue;
          this.fireForOperator(opId, Math.min(delta, EdgeTupleAnimationService.MAX_SPAWN_PER_TICK));
        }
      })
    );

    // Reset state across runs
    this.subscriptions.add(
      this.executeWorkflowService.getExecutionStateStream().subscribe(({ current }) => {
        if (current.state === ExecutionState.Initializing || current.state === ExecutionState.Uninitialized) {
          this.clearAllBubbles();
          this.prevOutputCountByOperator.clear();
          this.latestFieldsByOperator.clear();
          this.lastPagRequestAt.clear();
        }
      })
    );
  }

  /**
   * Update the content cache and backfill any in-flight bubbles whose source
   * operator just got fresh content, so dots become content pills mid-flight.
   */
  private cacheFieldsForOp(operatorID: string, fields: string[]): void {
    if (fields.length === 0) return;
    this.latestFieldsByOperator.set(operatorID, fields);
    for (const b of this.active) {
      if (b.sourceOperatorID !== operatorID) continue;
      const newKey = fields.join(", ");
      if (b.contentKey === newKey) continue;
      this.replaceBubbleVisual(b, fields);
      b.contentKey = newKey;
    }
  }

  private fireForOperator(operatorID: string, count: number): void {
    if (!this.paper) return;
    const outLinks = this.workflowActionService.getTexeraGraph().getOutputLinksByOperatorId(operatorID);
    if (outLinks.length === 0) return;
    const fields = this.latestFieldsByOperator.get(operatorID);
    for (const link of outLinks) {
      const inflight = this.inflightCountByLink.get(link.linkID) ?? 0;
      const room = EdgeTupleAnimationService.MAX_INFLIGHT_PER_LINK - inflight;
      const toSpawn = Math.min(count, Math.max(room, 0));
      for (let i = 0; i < toSpawn; i++) {
        this.spawnBubble(link.linkID, operatorID, fields, i * EdgeTupleAnimationService.STAGGER_MS);
      }
    }
  }

  private spawnBubble(linkID: string, sourceOperatorID: string, fields: string[] | undefined, delayMs: number): void {
    if (!this.paper || !this.overlayLayer) return;
    const jointLink = this.paper.getModelById(linkID) as joint.dia.Link | undefined;
    if (!jointLink) return;
    const linkView = this.paper.findViewByModel(jointLink) as joint.dia.LinkView | undefined;
    if (!linkView || !linkView.el) return;
    const pathEl = linkView.el.querySelector(".connection") as SVGPathElement | null;
    if (!pathEl) return;

    let pathLen = 0;
    try {
      pathLen = pathEl.getTotalLength();
    } catch {
      return;
    }
    if (pathLen < 1) return;

    const g = document.createElementNS(SVG_NS, "g") as SVGGElement;
    g.setAttribute("class", "tuple-bubble");
    g.setAttribute("opacity", "0");
    this.renderBubbleVisual(g, fields ?? []);
    this.overlayLayer.appendChild(g);

    const bubble: ActiveBubble = {
      el: g,
      start: performance.now() + delayMs,
      duration: EdgeTupleAnimationService.BUBBLE_DURATION_MS,
      pathEl,
      pathLen,
      linkID,
      sourceOperatorID,
      contentKey: fields && fields.length > 0 ? fields.join(", ") : "",
    };
    this.active.push(bubble);
    this.inflightCountByLink.set(linkID, (this.inflightCountByLink.get(linkID) ?? 0) + 1);
    this.ensureRafRunning();
  }

  /**
   * Replace a bubble's inner visual without unmounting the outer `<g>` so the
   * in-flight animation transform continues uninterrupted.
   */
  private replaceBubbleVisual(bubble: ActiveBubble, fields: string[]): void {
    while (bubble.el.firstChild) bubble.el.removeChild(bubble.el.firstChild);
    this.renderBubbleVisual(bubble.el, fields);
  }

  /**
   * Render mode by field count:
   *   0 → small filled circle ("dot")
   *   1 → plain text (no background)
   *   2+ → rounded-rect pill with white text
   */
  private renderBubbleVisual(g: SVGGElement, fields: string[]): void {
    if (fields.length === 0) {
      const dot = document.createElementNS(SVG_NS, "circle");
      dot.setAttribute("r", "3.5");
      dot.setAttribute("cx", "0");
      dot.setAttribute("cy", "0");
      dot.setAttribute("fill", "#1976d2");
      g.appendChild(dot);
      return;
    }

    const text = this.truncate(fields.join(", "), EdgeTupleAnimationService.MAX_TEXT_LEN);

    if (fields.length === 1) {
      // Bare text — no background pill, only the value.
      const t = document.createElementNS(SVG_NS, "text");
      t.setAttribute("x", "0");
      t.setAttribute("y", "0");
      t.setAttribute("text-anchor", "middle");
      t.setAttribute("dominant-baseline", "central");
      t.setAttribute("font-family", "Roboto, Arial, sans-serif");
      t.setAttribute("font-size", "12");
      t.setAttribute("font-weight", "600");
      t.setAttribute("fill", "#0d47a1");
      t.setAttribute("paint-order", "stroke");
      t.setAttribute("stroke", "#ffffff");
      t.setAttribute("stroke-width", "3");
      t.setAttribute("stroke-linejoin", "round");
      t.style.userSelect = "none";
      t.textContent = text;
      g.appendChild(t);
      return;
    }

    // Pill — multiple fields.
    const padding = 8;
    const approxCharW = 6.6;
    const w = Math.max(22, text.length * approxCharW + padding * 2);
    const h = 18;

    const rect = document.createElementNS(SVG_NS, "rect");
    rect.setAttribute("x", String(-w / 2));
    rect.setAttribute("y", String(-h / 2));
    rect.setAttribute("width", String(w));
    rect.setAttribute("height", String(h));
    rect.setAttribute("rx", "9");
    rect.setAttribute("ry", "9");
    rect.setAttribute("fill", "#1976d2");
    rect.setAttribute("stroke", "#0d47a1");
    rect.setAttribute("stroke-width", "0.6");

    const t = document.createElementNS(SVG_NS, "text");
    t.setAttribute("x", "0");
    t.setAttribute("y", "0");
    t.setAttribute("text-anchor", "middle");
    t.setAttribute("dominant-baseline", "central");
    t.setAttribute("font-family", "Roboto, Arial, sans-serif");
    t.setAttribute("font-size", "11");
    t.setAttribute("font-weight", "600");
    t.setAttribute("fill", "#ffffff");
    t.style.userSelect = "none";
    t.textContent = text;

    g.appendChild(rect);
    g.appendChild(t);
  }

  private ensureRafRunning(): void {
    if (this.rafHandle !== null) return;
    const step = (now: number) => {
      this.tick(now);
      if (this.active.length > 0) {
        this.rafHandle = requestAnimationFrame(step);
      } else {
        this.rafHandle = null;
      }
    };
    this.rafHandle = requestAnimationFrame(step);
  }

  private tick(now: number): void {
    for (let i = this.active.length - 1; i >= 0; i--) {
      const b = this.active[i];
      const elapsed = now - b.start;
      if (elapsed < 0) continue;
      const t = Math.min(1, elapsed / b.duration);
      let pt: { x: number; y: number };
      try {
        pt = b.pathEl.getPointAtLength(t * b.pathLen);
      } catch {
        this.disposeBubble(i);
        continue;
      }
      let opacity = 0.95;
      if (t < 0.18) opacity = 0.95 * (t / 0.18);
      else if (t > 0.82) opacity = 0.95 * ((1 - t) / 0.18);
      b.el.setAttribute("transform", `translate(${pt.x},${pt.y})`);
      b.el.setAttribute("opacity", String(opacity));
      if (t >= 1) this.disposeBubble(i);
    }
  }

  private disposeBubble(index: number): void {
    const b = this.active[index];
    b.el.parentNode?.removeChild(b.el);
    this.active.splice(index, 1);
    const cur = this.inflightCountByLink.get(b.linkID) ?? 0;
    if (cur <= 1) this.inflightCountByLink.delete(b.linkID);
    else this.inflightCountByLink.set(b.linkID, cur - 1);
  }

  private clearAllBubbles(): void {
    for (const b of this.active) {
      b.el.parentNode?.removeChild(b.el);
    }
    this.active = [];
    this.inflightCountByLink.clear();
    if (this.rafHandle !== null) {
      cancelAnimationFrame(this.rafHandle);
      this.rafHandle = null;
    }
  }

  /**
   * Send a paginated fetch for op X's last page so we get an actual row to cache.
   * Rate-limited per operator. The result panel uses requestID correlation, so
   * our requests don't collide with its in-flight requests; both subscribers
   * receive PaginatedResultEvent.
   */
  private requestLatestTupleFor(operatorID: string, totalNumTuples: number): void {
    const now = performance.now();
    const last = this.lastPagRequestAt.get(operatorID) ?? -Infinity;
    if (now - last < EdgeTupleAnimationService.PAG_REQUEST_INTERVAL_MS) return;
    this.lastPagRequestAt.set(operatorID, now);
    const pageSize = 1;
    const lastPage = Math.max(1, Math.ceil(totalNumTuples / pageSize));
    this.workflowWebsocketService.send("ResultPaginationRequest", {
      requestID: `edge-tuple-${operatorID}-${Math.floor(now)}`,
      operatorID,
      pageIndex: lastPage,
      pageSize,
    });
  }

  private fieldsFromRow(row: IndexableObject): string[] {
    const values: string[] = [];
    for (const k of Object.keys(row)) {
      const v = (row as Record<string, unknown>)[k];
      if (v === null || v === undefined) continue;
      const s = String(v);
      if (s === "" || s === "NULL") continue;
      values.push(s);
      if (values.length >= EdgeTupleAnimationService.MAX_FIELDS_IN_BUBBLE) break;
    }
    return values;
  }

  private fieldsFromArray(fields: ReadonlyArray<string>): string[] {
    const values: string[] = [];
    for (const f of fields) {
      if (!f || f === "NULL") continue;
      values.push(f);
      if (values.length >= EdgeTupleAnimationService.MAX_FIELDS_IN_BUBBLE) break;
    }
    return values;
  }

  private truncate(s: string, n: number): string {
    return s.length <= n ? s : s.slice(0, n - 1) + "…";
  }
}
