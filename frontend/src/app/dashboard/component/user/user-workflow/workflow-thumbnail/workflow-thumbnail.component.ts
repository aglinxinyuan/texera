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

import { Component, Input, OnChanges } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { WorkflowContent } from "../../../../../common/type/workflow";

interface RenderedOp {
  id: string;
  x: number;
  y: number;
  color: string;
}

interface RenderedLink {
  d: string;
}

/**
 * Renders a small static SVG preview of a workflow graph from its saved
 * WorkflowContent. Operators become colored rounded rects laid out by their
 * saved (x, y); links become smooth bezier curves between operator centers.
 * preserveAspectRatio fits the graph into the host element regardless of its
 * size, so the same component scales fluidly from list-thumbnail to popover.
 */
@Component({
  selector: "texera-workflow-thumbnail",
  standalone: true,
  imports: [NgFor, NgIf],
  templateUrl: "./workflow-thumbnail.component.html",
  styleUrls: ["./workflow-thumbnail.component.scss"],
})
export class WorkflowThumbnailComponent implements OnChanges {
  @Input() content?: WorkflowContent | null;

  readonly OP_WIDTH = 60;
  readonly OP_HEIGHT = 30;
  private readonly PADDING = 20;

  viewBox = "0 0 100 60";
  renderedOps: RenderedOp[] = [];
  renderedLinks: RenderedLink[] = [];
  isEmpty = true;

  ngOnChanges(): void {
    this.render();
  }

  private render(): void {
    const content = this.content;
    if (!content || !content.operators || content.operators.length === 0) {
      this.isEmpty = true;
      this.renderedOps = [];
      this.renderedLinks = [];
      return;
    }

    const positions = content.operatorPositions ?? {};
    const operators = content.operators;

    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    for (const op of operators) {
      const pos = positions[op.operatorID];
      if (!pos) continue;
      minX = Math.min(minX, pos.x);
      minY = Math.min(minY, pos.y);
      maxX = Math.max(maxX, pos.x + this.OP_WIDTH);
      maxY = Math.max(maxY, pos.y + this.OP_HEIGHT);
    }
    if (!isFinite(minX)) {
      this.isEmpty = true;
      this.renderedOps = [];
      this.renderedLinks = [];
      return;
    }

    minX -= this.PADDING;
    minY -= this.PADDING;
    maxX += this.PADDING;
    maxY += this.PADDING;
    const w = maxX - minX;
    const h = maxY - minY;
    this.viewBox = `${minX} ${minY} ${w} ${h}`;

    this.renderedOps = operators
      .filter(op => positions[op.operatorID])
      .map(op => ({
        id: op.operatorID,
        x: positions[op.operatorID].x,
        y: positions[op.operatorID].y,
        color: this.colorForType(op.operatorType),
      }));

    this.renderedLinks = (content.links ?? [])
      .map(link => {
        const s = positions[link.source.operatorID];
        const t = positions[link.target.operatorID];
        if (!s || !t) return null;
        const sx = s.x + this.OP_WIDTH;
        const sy = s.y + this.OP_HEIGHT / 2;
        const tx = t.x;
        const ty = t.y + this.OP_HEIGHT / 2;
        const cx = (sx + tx) / 2;
        return { d: `M${sx},${sy} C${cx},${sy} ${cx},${ty} ${tx},${ty}` };
      })
      .filter((l): l is RenderedLink => l !== null);

    this.isEmpty = false;
  }

  /** Deterministic hue from operator type — same type always gets the same color. */
  private colorForType(type: string): string {
    let h = 0;
    for (let i = 0; i < type.length; i++) h = ((h << 5) - h + type.charCodeAt(i)) | 0;
    const hue = ((h % 360) + 360) % 360;
    return `hsl(${hue}, 65%, 60%)`;
  }
}
