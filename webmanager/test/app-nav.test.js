import { test } from 'node:test';
import assert from 'node:assert/strict';

import { computeActiveSection, computeNavigateTop } from '../src/app-nav.js';

// 区块绝对位置（页面顶部起）
const absoluteTop = { overview: 0, bandwidth: 620, interfaces: 1800, flows: 2600, limits: 3800 };
const ids = ['overview', 'bandwidth', 'interfaces', 'flows', 'limits'];

// 构造给定滚动位置下各区块的 rect top（相对视口）
function sectionsAt(scrollY) {
  return ids.map((id) => ({ id, top: absoluteTop[id] - scrollY }));
}

const viewport = { scrollY: 0, innerHeight: 800, scrollHeight: 5000 };

test('computeActiveSection selects overview at top', () => {
  assert.equal(computeActiveSection(sectionsAt(0), viewport), 'overview');
});

test('computeActiveSection follows probe line through sections', () => {
  // 滚动 400：probe = 400 + 280 = 680；bandwidth 绝对位置 620 ≤ 680
  assert.equal(computeActiveSection(sectionsAt(400), { ...viewport, scrollY: 400 }), 'bandwidth');
  // 滚动 1200：probe = 1480；interfaces(1800) 未越过 → bandwidth
  assert.equal(computeActiveSection(sectionsAt(1200), { ...viewport, scrollY: 1200 }), 'bandwidth');
  // 滚动 1700：probe = 1980；interfaces(1800) 已越过 → interfaces
  assert.equal(computeActiveSection(sectionsAt(1700), { ...viewport, scrollY: 1700 }), 'interfaces');
  // 滚动 2400：probe = 2680；flows(2600) 已越过 → flows
  assert.equal(computeActiveSection(sectionsAt(2400), { ...viewport, scrollY: 2400 }), 'flows');
});

test('computeActiveSection selects last section when scrolled to bottom', () => {
  // 滚动到底 4200（5000-800）：limits(3800) 越过探测线 → limits
  assert.equal(
    computeActiveSection(sectionsAt(4200), { ...viewport, scrollY: 4200 }),
    'limits',
  );
  // 页面总高低到最后一块无法越过探测线（如末尾大留白）时仍选中最后一块
  assert.equal(
    computeActiveSection(sectionsAt(4380), { ...viewport, scrollY: 4380 }),
    'limits',
  );
});

test('computeActiveSection tolerates empty sections', () => {
  assert.equal(computeActiveSection([], viewport), 'overview');
});

test('computeNavigateTop aligns section top below fixed topbar', () => {
  // 区块当前在视口下方 900px，滚动到 836（减去 64 偏移）
  assert.equal(computeNavigateTop(900, 0), 836);
  // 已在视口上方时结果为 0（不反向滚动）
  assert.equal(computeNavigateTop(-120, 2400), 2216);
  assert.equal(computeNavigateTop(-120, 50), 0);
});
