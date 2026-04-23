"use client";

import { useState } from "react";
import dynamic from "next/dynamic";
import FilterBar from "@/components/FilterBar";

// Dynamic import prevents ArcGIS SDK from loading during server-side rendering.
// ArcGIS uses browser-only APIs (window, document, WebGL) that would crash
// the Node.js server process if imported at build time.
const MapView = dynamic(() => import("@/components/MapView"), {
  ssr: false,
  loading: () => (
    <div className="w-full h-full flex items-center justify-center bg-slate-950">
      <p className="text-slate-400">Loading map...</p>
    </div>
  ),
});

export default function MapPage() {
  const [stateFilter, setStateFilter] = useState<string | null>(null);

  return (
    // Full viewport height minus the navbar (56px / 3.5rem)
    <div className="relative w-full" style={{ height: "calc(100vh - 56px)" }}>
      <FilterBar stateFilter={stateFilter} onStateChange={setStateFilter} />
      <MapView stateFilter={stateFilter} />
    </div>
  );
}