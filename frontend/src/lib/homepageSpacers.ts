export interface HomepageSpacer {
  id: string;
  /** Null is before the first service; otherwise the preceding check/section key. */
  after: string | null;
}

export interface HomepageBlock {
  key: string;
}

/** Shared public/admin Services order: loose checks, paged tiles, then inline
 * groups in first-check order (including groups without section metadata). */
export function serviceBlocks<
  C extends { name: string; position?: number; group?: string },
  S extends { name: string; position: number; display: 'inline' | 'page'; collapsed?: boolean },
>(checks: C[], sections: S[], checkId: (check: C) => string) {
  const ordered = [...checks].sort(
    (a, b) => (a.position ?? 0) - (b.position ?? 0) || a.name.localeCompare(b.name),
  );
  const groups = new Map<string, C[]>();
  for (const check of ordered) {
    if (!check.group) continue;
    const existing = groups.get(check.group);
    if (existing) existing.push(check);
    else groups.set(check.group, [check]);
  }
  const sortedSections = [...sections].sort((a, b) => a.position - b.position);
  return [
    ...ordered.filter((check) => !check.group)
      .map((check) => ({ key: `check:${checkId(check)}`, kind: 'check' as const, check })),
    ...sortedSections.filter((section) => section.display === 'page')
      .map((section) => ({
        key: `section:${section.name}`, kind: 'section' as const,
        group: { name: section.name, checks: [] as C[], collapsed: section.collapsed ?? false, display: 'page' as const },
      })),
    ...[...groups].filter(([name]) => sections.find((section) => section.name === name)?.display !== 'page')
      .map(([name, checks]) => ({
        key: `section:${name}`, kind: 'section' as const,
        group: { name, checks, collapsed: sections.find((section) => section.name === name)?.collapsed ?? false, display: 'inline' as const },
      })),
  ];
}

export type HomepageItem<T extends HomepageBlock> =
  | { kind: 'block'; key: string; block: T }
  | { kind: 'spacer'; key: string; spacer: HomepageSpacer };

/** Keep the existing check/section order authoritative. Anchors follow their block
 * when that list is reordered; deleted blocks leave their spacers at the end. */
export function placeSpacers<T extends HomepageBlock>(
  blocks: T[], spacers: HomepageSpacer[],
): HomepageItem<T>[] {
  const items: HomepageItem<T>[] = [];
  const known = new Set(blocks.map((block) => block.key));
  const add = (after: string | null) => {
    for (const spacer of spacers) {
      if (spacer.after === after) items.push({ kind: 'spacer', key: `spacer:${spacer.id}`, spacer });
    }
  };
  add(null);
  for (const block of blocks) {
    items.push({ kind: 'block', key: block.key, block });
    add(block.key);
  }
  for (const spacer of spacers) {
    if (spacer.after !== null && !known.has(spacer.after)) {
      items.push({ kind: 'spacer', key: `spacer:${spacer.id}`, spacer });
    }
  }
  return items;
}

/** Move one layout-only row one slot, then derive anchors from the unchanged blocks. */
export function moveSpacer<T extends HomepageBlock>(
  blocks: T[], spacers: HomepageSpacer[], id: string, delta: number,
): HomepageSpacer[] {
  const items = placeSpacers(blocks, spacers);
  const from = items.findIndex((item) => item.kind === 'spacer' && item.spacer.id === id);
  const to = from + delta;
  if (from < 0 || to < 0 || to >= items.length) return spacers;
  const [moving] = items.splice(from, 1);
  items.splice(to, 0, moving);
  let after: string | null = null;
  const result: HomepageSpacer[] = [];
  for (const item of items) {
    if (item.kind === 'block') after = item.key;
    else result.push({ id: item.spacer.id, after });
  }
  return result;
}
