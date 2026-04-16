/**
 * Monochrome line icons, 1.5px stroke, 24×24 viewBox.
 * One consistent family (outline, never mixed with filled).
 */
type P = { size?: number; className?: string };

const SVG = ({ size = 16, className = "", children }: P & { children: React.ReactNode }) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.5"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
    aria-hidden
  >
    {children}
  </svg>
);

export const IconHome = (p: P) => (
  <SVG {...p}>
    <path d="M3 11.5 12 4l9 7.5" />
    <path d="M5 10v10h14V10" />
  </SVG>
);
export const IconActivity = (p: P) => (
  <SVG {...p}>
    <path d="M3 12h4l3-8 4 16 3-8h4" />
  </SVG>
);
export const IconLayers = (p: P) => (
  <SVG {...p}>
    <path d="M12 3 2 8l10 5 10-5-10-5Z" />
    <path d="M2 13l10 5 10-5" />
    <path d="M2 18l10 5 10-5" />
  </SVG>
);
export const IconList = (p: P) => (
  <SVG {...p}>
    <path d="M8 6h13" />
    <path d="M8 12h13" />
    <path d="M8 18h13" />
    <circle cx="3.5" cy="6" r="0.8" fill="currentColor" stroke="none" />
    <circle cx="3.5" cy="12" r="0.8" fill="currentColor" stroke="none" />
    <circle cx="3.5" cy="18" r="0.8" fill="currentColor" stroke="none" />
  </SVG>
);
export const IconSliders = (p: P) => (
  <SVG {...p}>
    <path d="M4 6h10" />
    <path d="M20 6h-2" />
    <path d="M4 18h6" />
    <path d="M20 18h-6" />
    <path d="M4 12h14" />
    <path d="M20 12h0" />
    <circle cx="16" cy="6" r="2" />
    <circle cx="12" cy="18" r="2" />
  </SVG>
);
export const IconPause = (p: P) => (
  <SVG {...p}>
    <rect x="6" y="5" width="4" height="14" rx="0.5" />
    <rect x="14" y="5" width="4" height="14" rx="0.5" />
  </SVG>
);
export const IconPlay = (p: P) => (
  <SVG {...p}>
    <path d="M7 5v14l12-7Z" />
  </SVG>
);
export const IconStop = (p: P) => (
  <SVG {...p}>
    <rect x="6" y="6" width="12" height="12" rx="1" />
  </SVG>
);
export const IconRetry = (p: P) => (
  <SVG {...p}>
    <path d="M20 11a8 8 0 1 1-2.5-5.8" />
    <path d="M20 4v5h-5" />
  </SVG>
);
export const IconRefresh = (p: P) => (
  <SVG {...p}>
    <path d="M4 12a8 8 0 0 1 14-5.3" />
    <path d="M20 4v5h-5" />
    <path d="M20 12a8 8 0 0 1-14 5.3" />
    <path d="M4 20v-5h5" />
  </SVG>
);
export const IconChevronRight = (p: P) => (
  <SVG {...p}>
    <path d="m9 6 6 6-6 6" />
  </SVG>
);
export const IconChevronDown = (p: P) => (
  <SVG {...p}>
    <path d="m6 9 6 6 6-6" />
  </SVG>
);
export const IconSend = (p: P) => (
  <SVG {...p}>
    <path d="M4 12 20 4l-8 16-2-6-6-2Z" />
  </SVG>
);
export const IconSearch = (p: P) => (
  <SVG {...p}>
    <circle cx="11" cy="11" r="6" />
    <path d="m20 20-4-4" />
  </SVG>
);
