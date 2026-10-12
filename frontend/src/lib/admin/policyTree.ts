/** One visible row of a tree drawn like `tree(1)`: what to put in each
 *  indentation column so parents visibly own their children.
 *
 *  `guides[i]` is the column for the ancestor at depth `i + 1`: true when that
 *  ancestor has a later sibling, so a `│` runs past this row down to it. The
 *  row's own column is `├──`, or `└──` when `last`. The root (depth 0) has no
 *  columns at all. */
export type Branch<T> = {
  item: T;
  depth: number;
  guides: boolean[];
  last: boolean;
  /** Whether the item has children at all, open or not — it decides between
   *  an expand toggle and a leaf marker. */
  hasChildren: boolean;
  /** Children are drawn below this row. */
  open: boolean;
};

export function flattenTree<T>(
  root: T,
  childrenOf: (item: T) => T[],
  isOpen: (item: T) => boolean,
): Branch<T>[] {
  const out: Branch<T>[] = [];
  const walk = (item: T, depth: number, guides: boolean[], last: boolean) => {
    const kids = childrenOf(item);
    const open = kids.length > 0 && isOpen(item);
    out.push({ item, depth, guides, last, hasChildren: kids.length > 0, open });
    if (!open) return;
    const childGuides = depth === 0 ? [] : [...guides, !last];
    kids.forEach((kid, i) => walk(kid, depth + 1, childGuides, i === kids.length - 1));
  };
  walk(root, 0, [], true);
  return out;
}
