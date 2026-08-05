/**
 * 侧边导航的滚动定位纯函数。
 *
 * 从 App.vue 抽出以便确定性测试：计算当前激活区块与导航目标滚动位置，
 * 不依赖 DOM 副作用。
 */

/**
 * 根据滚动位置计算激活的导航区块。
 *
 * @param {{id: string, top: number}[]} sections 区块列表；top 为区块相对
 *   视口顶部的距离（getBoundingClientRect().top）。
 * @param {{scrollY: number, innerHeight: number, scrollHeight: number}} viewport
 *   滚动位置与视口尺寸。
 * @returns {string} 激活区块的 id；sections 为空时返回 'overview'。
 */
export function computeActiveSection(sections, { scrollY, innerHeight, scrollHeight }) {
  const probe = scrollY + innerHeight * 0.35;
  let current = sections[0]?.id ?? 'overview';
  for (const s of sections) {
    if (s.top + scrollY <= probe) current = s.id;
  }
  // 滚动到底部时选中最后一个区块：最后一块可能因底部对齐而未越过探测线
  if (sections.length > 0 && scrollY + innerHeight >= scrollHeight - 2) {
    current = sections[sections.length - 1].id;
  }
  return current;
}

/**
 * 计算导航点击后的目标滚动位置：使区块顶部对齐视口顶部偏移 offset。
 *
 * @param {number} rectTop 区块相对视口顶部的距离。
 * @param {number} scrollY 当前滚动位置。
 * @param {number} offset 顶部固定栏预留偏移（像素）。
 * @returns {number} 目标 scrollY，不小于 0。
 */
export function computeNavigateTop(rectTop, scrollY, offset = 64) {
  return Math.max(0, rectTop + scrollY - offset);
}
