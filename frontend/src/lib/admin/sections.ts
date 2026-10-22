import { send, request } from "./client";
import type { HomepageSpacer } from "../homepageSpacers";

/** A named, ordered group on the status page. Exists independently of checks,
 *  so an empty section can be created before anything is put in it. */
export interface Section {
  name: string;
  position: number;
  /** The PUBLIC status page renders this section collapsed on first load. The
   *  admin view ignores it, and a paged section has nothing to collapse. */
  collapsed: boolean;
  /** How the PUBLIC status page renders this section: its rows inline, or a
   *  tile linking to its own page. */
  display: SectionDisplay;
}

export type SectionDisplay = "inline" | "page";

export async function fetchSections(): Promise<Section[]> {
  const r = await request<{ sections: Section[] }>("/api/v1/admin/sections");
  return r.sections;
}

export async function createSection(name: string): Promise<Section> {
  const r = await send<{ section: Section }>("POST", "/api/v1/admin/sections", {
    name,
  });
  return r.section;
}

/** Rename and/or set the presentation flags. A rename moves every check in the
 *  section with it — callers do not patch checks. An omitted `collapsed` or
 *  `display` is left as it is, so this one call serves the rename and both
 *  toggles without one of them clobbering the others. */
export async function updateSection(
  current: string,
  next: { name?: string; collapsed?: boolean; display?: SectionDisplay },
): Promise<Section> {
  const r = await send<{ section: Section }>(
    "PUT",
    `/api/v1/admin/sections/${encodeURIComponent(current)}`,
    { name: next.name ?? current, collapsed: next.collapsed, display: next.display },
  );
  return r.section;
}

/** Its checks survive and become loose. */
export async function deleteSection(name: string): Promise<void> {
  await send("DELETE", `/api/v1/admin/sections/${encodeURIComponent(name)}`);
}

export async function reorderSections(order: string[]): Promise<Section[]> {
  const r = await send<{ sections: Section[] }>(
    "PUT",
    "/api/v1/admin/sections",
    { order },
  );
  return r.sections;
}

export async function saveHomepageSpacers(spacers: HomepageSpacer[]): Promise<HomepageSpacer[]> {
  const r = await send<{ spacers: HomepageSpacer[] }>(
    "PUT", "/api/v1/admin/homepage/spacers", { spacers },
  );
  return r.spacers;
}
