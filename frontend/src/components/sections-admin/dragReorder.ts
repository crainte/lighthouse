// Native HTML5 drag carries no usable payload between elements, so the source
// row lives in component state; these are the pure parts shared by the two
// drag lists on this page (sections, and checks within a section).

export type DragState = { list: string; from: number; over: number | null };

/** Which side of the hovered row the dragged one will land on. Dragging down
 *  lands after the target, dragging up lands before it. */
export function dropEdge(
  drag: DragState | null,
  list: string,
  index: number,
): 'above' | 'below' | null {
  if (!drag || drag.list !== list || drag.over !== index) return null;
  return drag.from < index ? 'below' : 'above';
}

export function reorder<T>(items: T[], from: number, to: number): T[] {
  const next = [...items];
  const [moved] = next.splice(from, 1);
  next.splice(to, 0, moved);
  return next;
}
