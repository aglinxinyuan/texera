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
import { ExecutionState } from "../../types/execute-workflow.interface";

const SVG_NS = "http://www.w3.org/2000/svg";

interface Particle {
  el: SVGCircleElement;
  start: number;
  duration: number;
  pathEl: SVGPathElement;
  pathLen: number;
  linkID: string;
}

interface Bloom {
  el: SVGCircleElement;
  start: number;
  duration: number;
}

/**
 * "Particle Storm" edge animation.
 *
 * Every edge continuously emits glowing particles along its path while the
 * workflow runs. Emission rate per edge tracks the source operator's recent
 * rows/sec, smoothed and decayed. Each operator pulses a colored bloom ring
 * whenever it ships a batch. Particles inherit a deterministic hue from their
 * source operator ID so streams from different sources stay visually distinct
 * even where they converge.
 */
@Injectable({ providedIn: "root" })
export class EdgeTupleAnimationService {
  private static readonly PARTICLE_DURATION_MS = 1500;
  private static readonly PARTICLE_RADIUS = 2.6;
  private static readonly MAX_PARTICLES = 600;
  private static readonly RATE_SMOOTH_ALPHA = 0.35; // EMA factor for rate updates
  private static readonly RATE_DECAY_AFTER_MS = 1800; // after this long with no update, decay rate
  private static readonly MIN_RATE_FOR_EMIT = 0.25; // particles/sec; below this, no emission
  private static readonly MAX_RATE_PER_EDGE = 35; // cap to avoid melting the browser
  private static readonly BLOOM_DURATION_MS = 700;
  private static readonly BLOOM_MAX_RADIUS = 60;

  private paper: joint.dia.Paper | null = null;
  private overlayLayer: SVGGElement | null = null;
  private enabled = true;
  private debug = false;

  private operatorOutputCount = new Map<string, number>();
  private operatorRowsPerSec = new Map<string, number>();
  private operatorLastUpdateAt = new Map<string, number>();
  private edgeLastEmitAt = new Map<string, number>();

  private particles: Particle[] = [];
  private blooms: Bloom[] = [];
  private rafHandle: number | null = null;

  private subscriptions = new Subscription();

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
    private workflowActionService: WorkflowActionService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    if (typeof window !== "undefined") (window as any).edgeTuple = this;
  }

  public setEnabled(on: boolean): void {
    this.enabled = on;
    if (!on) this.clearAll();
  }

  public isEnabled(): boolean {
    return this.enabled;
  }

  public setDebug(on: boolean): void {
    this.debug = on;
  }

  public dumpState(): object {
    return {
      enabled: this.enabled,
      operators: Object.fromEntries(this.operatorRowsPerSec),
      particles: this.particles.length,
      blooms: this.blooms.length,
    };
  }

  public attachToPaper(paper: joint.dia.Paper): void {
    if (this.paper === paper && this.overlayLayer && this.overlayLayer.isConnected) return;
    this.clearAll();
    this.paper = paper;
    this.ensureGlowFilter(paper.svg as SVGSVGElement);
    this.overlayLayer = this.createOverlayLayer(paper);
    this.subscribe();
  }

  private ensureGlowFilter(svg: SVGSVGElement): void {
    let defs = svg.querySelector("defs") as SVGDefsElement | null;
    if (!defs) {
      defs = document.createElementNS(SVG_NS, "defs") as SVGDefsElement;
      svg.insertBefore(defs, svg.firstChild);
    }
    if (defs.querySelector("#et-glow")) return;
    const filter = document.createElementNS(SVG_NS, "filter");
    filter.setAttribute("id", "et-glow");
    filter.setAttribute("x", "-100%");
    filter.setAttribute("y", "-100%");
    filter.setAttribute("width", "300%");
    filter.setAttribute("height", "300%");
    const blur = document.createElementNS(SVG_NS, "feGaussianBlur");
    blur.setAttribute("stdDeviation", "2.4");
    blur.setAttribute("result", "blur");
    const merge = document.createElementNS(SVG_NS, "feMerge");
    const m1 = document.createElementNS(SVG_NS, "feMergeNode");
    m1.setAttribute("in", "blur");
    const m2 = document.createElementNS(SVG_NS, "feMergeNode");
    m2.setAttribute("in", "blur");
    const m3 = document.createElementNS(SVG_NS, "feMergeNode");
    m3.setAttribute("in", "SourceGraphic");
    merge.appendChild(m1);
    merge.appendChild(m2);
    merge.appendChild(m3);
    filter.appendChild(blur);
    filter.appendChild(merge);
    defs.appendChild(filter);
  }

  private createOverlayLayer(paper: joint.dia.Paper): SVGGElement {
    const svg = paper.svg as SVGSVGElement;
    const cellsLayer = svg.querySelector(".joint-cells-layer") as SVGGElement | null;
    const host: SVGGElement | SVGSVGElement = cellsLayer ?? svg;
    const layer = document.createElementNS(SVG_NS, "g");
    layer.setAttribute("class", "tuple-animation-layer");
    layer.setAttribute("filter", "url(#et-glow)");
    layer.style.pointerEvents = "none";
    host.appendChild(layer);
    return layer;
  }

  private subscribe(): void {
    this.subscriptions.unsubscribe();
    this.subscriptions = new Subscription();

    // Throughput source: per-operator output count.
    this.subscriptions.add(
      this.workflowStatusService.getStatusUpdateStream().subscribe(stats => {
        if (!this.enabled || !this.paper) return;
        const now = performance.now();
        for (const opId of Object.keys(stats)) {
          const current = stats[opId].aggregatedOutputRowCount ?? 0;
          const prev = this.operatorOutputCount.get(opId) ?? 0;
          const prevAt = this.operatorLastUpdateAt.get(opId) ?? now;
          this.operatorOutputCount.set(opId, current);
          this.operatorLastUpdateAt.set(opId, now);

          const delta = current - prev;
          const dtSec = Math.max((now - prevAt) / 1000, 0.001);
          const instRate = delta / dtSec;
          const prevSmooth = this.operatorRowsPerSec.get(opId) ?? 0;
          const smoothed =
            prevSmooth * (1 - EdgeTupleAnimationService.RATE_SMOOTH_ALPHA) +
            instRate * EdgeTupleAnimationService.RATE_SMOOTH_ALPHA;
          this.operatorRowsPerSec.set(opId, Math.max(0, smoothed));

          if (delta > 0) this.triggerBloomFor(opId);
        }
        this.ensureRafRunning();
      })
    );

    // Reset across runs.
    this.subscriptions.add(
      this.executeWorkflowService.getExecutionStateStream().subscribe(({ current }) => {
        if (current.state === ExecutionState.Initializing || current.state === ExecutionState.Uninitialized) {
          this.clearAll();
          this.operatorOutputCount.clear();
          this.operatorRowsPerSec.clear();
          this.operatorLastUpdateAt.clear();
          this.edgeLastEmitAt.clear();
        }
      })
    );
  }

  private triggerBloomFor(operatorID: string): void {
    if (!this.paper || !this.overlayLayer) return;
    const element = this.paper.getModelById(operatorID) as joint.dia.Element | undefined;
    if (!element) return;
    const bbox = element.getBBox();
    const cx = bbox.x + bbox.width / 2;
    const cy = bbox.y + bbox.height / 2;
    const color = this.colorForOperator(operatorID);

    const c = document.createElementNS(SVG_NS, "circle");
    c.setAttribute("cx", String(cx));
    c.setAttribute("cy", String(cy));
    c.setAttribute("r", "0");
    c.setAttribute("fill", "none");
    c.setAttribute("stroke", color);
    c.setAttribute("stroke-width", "2.5");
    c.setAttribute("opacity", "0.9");
    this.overlayLayer.appendChild(c);

    this.blooms.push({
      el: c,
      start: performance.now(),
      duration: EdgeTupleAnimationService.BLOOM_DURATION_MS,
    });
  }

  private ensureRafRunning(): void {
    if (this.rafHandle !== null) return;
    const step = (now: number) => {
      this.tick(now);
      if (this.shouldKeepAnimating()) {
        this.rafHandle = requestAnimationFrame(step);
      } else {
        this.rafHandle = null;
      }
    };
    this.rafHandle = requestAnimationFrame(step);
  }

  private shouldKeepAnimating(): boolean {
    if (this.particles.length > 0 || this.blooms.length > 0) return true;
    for (const rate of this.operatorRowsPerSec.values()) {
      if (rate >= EdgeTupleAnimationService.MIN_RATE_FOR_EMIT) return true;
    }
    return false;
  }

  private tick(now: number): void {
    if (!this.enabled) return;
    this.decayRates(now);
    this.emitForActiveEdges(now);
    this.advanceParticles(now);
    this.advanceBlooms(now);
  }

  private decayRates(now: number): void {
    for (const [opId, lastAt] of this.operatorLastUpdateAt) {
      const idle = now - lastAt;
      if (idle <= EdgeTupleAnimationService.RATE_DECAY_AFTER_MS) continue;
      const rate = this.operatorRowsPerSec.get(opId) ?? 0;
      if (rate <= 0) continue;
      // Exponential decay once idle.
      this.operatorRowsPerSec.set(opId, rate * 0.85);
    }
  }

  private emitForActiveEdges(now: number): void {
    if (!this.paper || !this.overlayLayer) return;
    if (this.particles.length >= EdgeTupleAnimationService.MAX_PARTICLES) return;

    const links = this.workflowActionService.getTexeraGraph().getAllLinks();
    for (const link of links) {
      const sourceOp = link.source.operatorID;
      const rateRaw = this.operatorRowsPerSec.get(sourceOp) ?? 0;
      if (rateRaw < EdgeTupleAnimationService.MIN_RATE_FOR_EMIT) continue;
      const rate = Math.min(rateRaw, EdgeTupleAnimationService.MAX_RATE_PER_EDGE);
      const intervalMs = 1000 / rate;
      const last = this.edgeLastEmitAt.get(link.linkID) ?? -Infinity;
      const elapsed = now - last;
      if (elapsed < intervalMs) continue;
      // Possibly emit multiple particles to catch up after a busy frame.
      const toEmit = Math.min(Math.floor(elapsed / intervalMs), 3);
      for (let i = 0; i < toEmit; i++) {
        this.emitParticle(link.linkID, sourceOp, now - elapsed + (i + 1) * intervalMs);
        if (this.particles.length >= EdgeTupleAnimationService.MAX_PARTICLES) break;
      }
      this.edgeLastEmitAt.set(link.linkID, now);
    }
  }

  private emitParticle(linkID: string, sourceOperatorID: string, startAt: number): void {
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

    const color = this.colorForOperator(sourceOperatorID);
    const c = document.createElementNS(SVG_NS, "circle");
    c.setAttribute("r", String(EdgeTupleAnimationService.PARTICLE_RADIUS));
    c.setAttribute("cx", "0");
    c.setAttribute("cy", "0");
    c.setAttribute("fill", color);
    c.setAttribute("opacity", "0");
    this.overlayLayer.appendChild(c);

    // Slight randomization for organic feel.
    const jitter = 0.85 + Math.random() * 0.3; // 0.85x – 1.15x duration
    this.particles.push({
      el: c,
      start: startAt,
      duration: EdgeTupleAnimationService.PARTICLE_DURATION_MS * jitter,
      pathEl,
      pathLen,
      linkID,
    });
  }

  private advanceParticles(now: number): void {
    for (let i = this.particles.length - 1; i >= 0; i--) {
      const p = this.particles[i];
      const elapsed = now - p.start;
      if (elapsed < 0) continue;
      const t = Math.min(1, elapsed / p.duration);
      let pt: { x: number; y: number };
      try {
        pt = p.pathEl.getPointAtLength(t * p.pathLen);
      } catch {
        this.disposeParticle(i);
        continue;
      }
      let opacity = 1;
      if (t < 0.1) opacity = t / 0.1;
      else if (t > 0.85) opacity = (1 - t) / 0.15;
      p.el.setAttribute("transform", `translate(${pt.x},${pt.y})`);
      p.el.setAttribute("opacity", String(opacity));
      if (t >= 1) this.disposeParticle(i);
    }
  }

  private advanceBlooms(now: number): void {
    for (let i = this.blooms.length - 1; i >= 0; i--) {
      const b = this.blooms[i];
      const t = Math.min(1, (now - b.start) / b.duration);
      const r = EdgeTupleAnimationService.BLOOM_MAX_RADIUS * t;
      const opacity = (1 - t) * 0.9;
      b.el.setAttribute("r", String(r));
      b.el.setAttribute("opacity", String(opacity));
      if (t >= 1) this.disposeBloom(i);
    }
  }

  private disposeParticle(index: number): void {
    const p = this.particles[index];
    p.el.parentNode?.removeChild(p.el);
    this.particles.splice(index, 1);
  }

  private disposeBloom(index: number): void {
    const b = this.blooms[index];
    b.el.parentNode?.removeChild(b.el);
    this.blooms.splice(index, 1);
  }

  private clearAll(): void {
    for (const p of this.particles) p.el.parentNode?.removeChild(p.el);
    for (const b of this.blooms) b.el.parentNode?.removeChild(b.el);
    this.particles = [];
    this.blooms = [];
    if (this.rafHandle !== null) {
      cancelAnimationFrame(this.rafHandle);
      this.rafHandle = null;
    }
  }

  private colorForOperator(opId: string): string {
    // Deterministic hue from operator ID — sibling streams stay visually distinct.
    let h = 0;
    for (let i = 0; i < opId.length; i++) {
      h = ((h << 5) - h + opId.charCodeAt(i)) | 0;
    }
    const hue = ((h % 360) + 360) % 360;
    return `hsl(${hue}, 100%, 65%)`;
  }
}
