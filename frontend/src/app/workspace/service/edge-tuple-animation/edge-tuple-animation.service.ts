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
import { ExecutionState, isWebDataUpdate } from "../../types/execute-workflow.interface";
import { IndexableObject } from "../../types/result-table.interface";

const SVG_NS = "http://www.w3.org/2000/svg";

interface ActiveBubble {
  el: SVGGElement;
  start: number;
  duration: number;
  pathEl: SVGPathElement;
  pathLen: number;
  linkID: string;
}

/**
 * Animates "tuple bubbles" along workflow edges while a workflow is running.
 *
 * Trigger: per-tick output-count delta from OperatorStatisticsUpdateEvent.
 * Content: latest tuple content cached from WebResultUpdateEvent (set mode),
 *          PaginatedResultEvent, or OperatorCurrentTuplesUpdateEvent (pause).
 *          Falls back to a "•" dot when no content is known yet.
 */
@Injectable({ providedIn: "root" })
export class EdgeTupleAnimationService {
  private static readonly MAX_INFLIGHT_PER_LINK = 4;
  private static readonly MAX_SPAWN_PER_TICK = 5;
  private static readonly BUBBLE_DURATION_MS = 1400;
  private static readonly STAGGER_MS = 140;
  private static readonly MAX_TEXT_LEN = 18;

  private paper: joint.dia.Paper | null = null;
  private overlayLayer: SVGGElement | null = null;
  private enabled = true;

  private latestContentByOperator = new Map<string, string>();
  private prevOutputCountByOperator = new Map<string, number>();
  private inflightCountByLink = new Map<string, number>();
  private active: ActiveBubble[] = [];
  private rafHandle: number | null = null;

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
    this.subscribe();
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
    // Avoid double-subscribing if attachToPaper is called again with the same paper.
    this.subscriptions.unsubscribe();
    this.subscriptions = new Subscription();

    // Content cache: WebResultUpdateEvent (live during running, for ops with materialized results)
    this.subscriptions.add(
      this.workflowWebsocketService.subscribeToEvent("WebResultUpdateEvent").subscribe(evt => {
        const updates = (evt as any).updates as Record<string, any> | undefined;
        if (!updates) return;
        for (const opId of Object.keys(updates)) {
          const u = updates[opId];
          if (u && isWebDataUpdate(u) && Array.isArray(u.table) && u.table.length > 0) {
            const row = u.table[u.table.length - 1] as IndexableObject;
            const repr = this.firstReadableField(row);
            if (repr) this.latestContentByOperator.set(opId, repr);
          }
        }
      })
    );

    // Content cache: PaginatedResultEvent (when result panel pages in)
    this.subscriptions.add(
      this.workflowWebsocketService.subscribeToEvent("PaginatedResultEvent").subscribe(evt => {
        if (evt.table && evt.table.length > 0) {
          const row = evt.table[evt.table.length - 1] as IndexableObject;
          const repr = this.firstReadableField(row);
          if (repr) this.latestContentByOperator.set(evt.operatorID, repr);
        }
      })
    );

    // Content cache: pause-time current tuples
    this.subscriptions.add(
      this.workflowWebsocketService.subscribeToEvent("OperatorCurrentTuplesUpdateEvent").subscribe(evt => {
        const sample = evt.tuples?.[0]?.tuple;
        if (sample && sample.length > 0) {
          const repr = this.firstNonEmpty(sample);
          if (repr) this.latestContentByOperator.set(evt.operatorID, repr);
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
          this.latestContentByOperator.clear();
        }
      })
    );
  }

  private fireForOperator(operatorID: string, count: number): void {
    if (!this.paper) return;
    const outLinks = this.workflowActionService.getTexeraGraph().getOutputLinksByOperatorId(operatorID);
    if (outLinks.length === 0) return;
    const content = this.latestContentByOperator.get(operatorID);
    for (const link of outLinks) {
      const inflight = this.inflightCountByLink.get(link.linkID) ?? 0;
      const room = EdgeTupleAnimationService.MAX_INFLIGHT_PER_LINK - inflight;
      const toSpawn = Math.min(count, Math.max(room, 0));
      for (let i = 0; i < toSpawn; i++) {
        this.spawnBubble(link.linkID, content, i * EdgeTupleAnimationService.STAGGER_MS);
      }
    }
  }

  private spawnBubble(linkID: string, content: string | undefined, delayMs: number): void {
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

    const text = content ? this.truncate(content, EdgeTupleAnimationService.MAX_TEXT_LEN) : "•";
    const g = this.buildBubbleElement(text);
    this.overlayLayer.appendChild(g);

    const bubble: ActiveBubble = {
      el: g,
      start: performance.now() + delayMs,
      duration: EdgeTupleAnimationService.BUBBLE_DURATION_MS,
      pathEl,
      pathLen,
      linkID,
    };
    this.active.push(bubble);
    this.inflightCountByLink.set(linkID, (this.inflightCountByLink.get(linkID) ?? 0) + 1);
    this.ensureRafRunning();
  }

  private buildBubbleElement(text: string): SVGGElement {
    const g = document.createElementNS(SVG_NS, "g") as SVGGElement;
    g.setAttribute("class", "tuple-bubble");
    g.setAttribute("opacity", "0");

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
    return g;
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

  private firstReadableField(row: IndexableObject): string | undefined {
    for (const k of Object.keys(row)) {
      const v = (row as Record<string, unknown>)[k];
      if (v === null || v === undefined) continue;
      const s = String(v);
      if (s === "" || s === "NULL") continue;
      return `${k}=${s}`;
    }
    return undefined;
  }

  private firstNonEmpty(fields: ReadonlyArray<string>): string | undefined {
    for (const f of fields) {
      if (f && f !== "NULL") return f;
    }
    return undefined;
  }

  private truncate(s: string, n: number): string {
    return s.length <= n ? s : s.slice(0, n - 1) + "…";
  }
}
