"use client";

import { useEffect, useRef, useState } from "react";
import type { Resort } from "@/types";
import { getResort } from "@/lib/api";
import type { ResortDetail } from "@/types";
import ResortPanel from "./ResortPanel";

// ArcGIS SDK imports — these only run in the browser
import Map from "@arcgis/core/Map";
import MapView from "@arcgis/core/views/MapView";
import Basemap from "@arcgis/core/Basemap";
import GeoJSONLayer from "@arcgis/core/layers/GeoJSONLayer";
import esriConfig from "@arcgis/core/config";
import ClassBreaksRenderer from "@arcgis/core/renderers/ClassBreaksRenderer";
import SimpleMarkerSymbol from "@arcgis/core/symbols/SimpleMarkerSymbol";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

// Color ramp for condition scores: pale blue (poor) → deep purple (epic)
function getScoreSymbol(color: string, size: number): SimpleMarkerSymbol {
  return new SimpleMarkerSymbol({
    style: "circle",
    color,
    size,
    outline: { color: "rgba(255,255,255,0.3)", width: 1 },
  });
}

// Class breaks renderer — colors markers by condition_score
const conditionRenderer = new ClassBreaksRenderer({
  field: "condition_score",
  defaultSymbol: getScoreSymbol("#475569", 10), // slate for no data
  classBreakInfos: [
    {
      minValue: 0,
      maxValue: 19,
      symbol: getScoreSymbol("#94a3b8", 10), // slate-400
      label: "Poor (0-19)",
    },
    {
      minValue: 20,
      maxValue: 39,
      symbol: getScoreSymbol("#7dd3fc", 12), // sky-300
      label: "Fair (20-39)",
    },
    {
      minValue: 40,
      maxValue: 59,
      symbol: getScoreSymbol("#38bdf8", 14), // sky-400
      label: "Good (40-59)",
    },
    {
      minValue: 60,
      maxValue: 79,
      symbol: getScoreSymbol("#818cf8", 16), // indigo-400
      label: "Great (60-79)",
    },
    {
      minValue: 80,
      maxValue: 100,
      symbol: getScoreSymbol("#a78bfa", 18), // violet-400
      label: "Epic (80-100)",
    },
  ],
});

// Pop-up template shown when clicking a resort marker
const popupTemplate = {
  title: "{name}",
  content: [
    {
      type: "fields",
      fieldInfos: [
        { fieldName: "state", label: "State" },
        { fieldName: "condition_score", label: "Condition Score" },
        { fieldName: "current_snow_depth_in", label: "Snow Depth (in)" },
        { fieldName: "snowfall_48h_in", label: "48h Snowfall (in)" },
        { fieldName: "snowfall_7d_in", label: "7d Snowfall (in)" },
        { fieldName: "snowpack_trend", label: "Snowpack Trend" },
        { fieldName: "base_elevation_ft", label: "Base Elevation (ft)" },
        { fieldName: "summit_elevation_ft", label: "Summit Elevation (ft)" },
      ],
    },
  ],
};

interface MapComponentProps {
  onResortSelect?: (id: number) => void;
  stateFilter?: string | null;
}

export default function MapComponent({ onResortSelect, stateFilter }: MapComponentProps) {
  const mapDiv = useRef<HTMLDivElement>(null);
  const viewRef = useRef<MapView | null>(null);
  const layerRef = useRef<GeoJSONLayer | null>(null);
  const [selectedResort, setSelectedResort] = useState<ResortDetail | null>(null);
  const [panelOpen, setPanelOpen] = useState(false);

  useEffect(() => {
    if (!mapDiv.current) return;

    // Set the API key for basemap access (required for the v2 basemap styles service).
    const apiKey = process.env.NEXT_PUBLIC_ARCGIS_API_KEY || "";
    esriConfig.apiKey = apiKey;

    // Create the GeoJSON layer from your API's export endpoint
    const geojsonLayer = new GeoJSONLayer({
      url: `${API_URL}/export/geojson`,
      title: "Resort Conditions",
      renderer: conditionRenderer,
      popupTemplate,
      outFields: ["*"],
    });

    layerRef.current = geojsonLayer;

    // In @arcgis/core v5, "arcgis/*" IDs belong to the v2 Basemap Styles service
    // and must be passed via `style`, not as a plain basemap id string.
    // Fall back to OpenStreetMap (no key required) if no API key is configured
    // so the map still renders in local dev.
    const basemap = apiKey
      ? new Basemap({ style: { id: "arcgis/topographic" } })
      : Basemap.fromId("osm");

    const map = new Map({
      basemap,
      layers: [geojsonLayer],
    });

    // Create the map view centered on the western US
    const view = new MapView({
      container: mapDiv.current,
      map,
      center: [-119.5, 40.0], // Western US center
      zoom: 5,
      ui: {
        components: ["zoom", "compass"],
      },
      popup: {
        dockEnabled: false,
        dockOptions: { buttonEnabled: false },
      },
    });

    viewRef.current = view;

    // Handle click on resort features — open the detail panel
    view.on("click", async (event) => {
      const response = await view.hitTest(event);
      const result = response.results.find(
        (r) => r.type === "graphic" && r.graphic.layer === geojsonLayer
      );

      if (result && result.type === "graphic") {
        const resortId = result.graphic.attributes.id;
        if (resortId) {
          try {
            const detail = await getResort(resortId);
            setSelectedResort(detail);
            setPanelOpen(true);
          } catch (err) {
            console.error("Error fetching resort detail:", err);
          }
        }
      }
    });

    // Cleanup on unmount
    return () => {
      view.destroy();
    };
  }, []);

  // Apply state filter when it changes by updating the layer's definitionExpression
  useEffect(() => {
    if (!layerRef.current) return;
    if (stateFilter) {
      layerRef.current.definitionExpression = `state = '${stateFilter}'`;
    } else {
      layerRef.current.definitionExpression = "";
    }
  }, [stateFilter]);

  // Zoom to a state when filter changes
  useEffect(() => {
    if (!viewRef.current || !stateFilter) return;
    const stateCenter: Record<string, { center: [number, number]; zoom: number }> = {
      CA: { center: [-119.5, 38.5], zoom: 7 },
      CO: { center: [-106.2, 39.5], zoom: 7 },
      WA: { center: [-121.2, 47.5], zoom: 7 },
    };
    const target = stateCenter[stateFilter];
    if (target) {
      viewRef.current.goTo({ center: target.center, zoom: target.zoom }, { duration: 800 });
    }
  }, [stateFilter]);

  return (
    <div className="relative w-full h-full">
      {/* The map renders into this div */}
      <div ref={mapDiv} className="w-full h-full" />

      {/* Resort detail panel — slides in from the right */}
      {panelOpen && selectedResort && (
        <ResortPanel
          resort={selectedResort}
          onClose={() => {
            setPanelOpen(false);
            setSelectedResort(null);
          }}
        />
      )}
    </div>
  );
}