import normalScene from '../assets/lighthouse-normal.png';
import springScene from '../assets/lighthouse-spring.png';
import summerScene from '../assets/lighthouse-summer.png';
import fallScene from '../assets/lighthouse-fall.png';
import winterScene from '../assets/lighthouse-winter.png';
import type { Season } from './seasonalAtmosphere';

/** Shared seasonal art for both the Overview hero and the persistent sidebar.
 * Off/normal is intentionally not a blank background: it uses the base scene.
 */
export function sceneForSeason(season: Season | null): string {
  switch (season) {
    case 'spring': return springScene;
    case 'summer': return summerScene;
    case 'fall': return fallScene;
    case 'winter': return winterScene;
    default: return normalScene;
  }
}
