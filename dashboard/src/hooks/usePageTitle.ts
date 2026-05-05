import { useEffect } from "react";

export function usePageTitle(title: string) {
  useEffect(() => {
    const prev = document.title;
    document.title = `${title} · Orch8`;
    return () => {
      document.title = prev;
    };
  }, [title]);
}
