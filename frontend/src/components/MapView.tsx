"use client";

import { useEffect, useRef, useState } from "react";
import { getResort } from "@/lib/api";
import type { ResortDetail } from "@/types";
import ResortPanel from "./ResortPanel";

import Map from "@arcgis/core/Map";
import MapViewEsri from "@arcgis/core/views/MapView";
import Basemap from "@arcgis/core/Basemap";
import GeoJSONLayer from "@arcgis/core/layers/GeoJSONLayer";
import esriConfig from "@arcgis/core/config";
import UniqueValueRenderer from "@arcgis/core/renderers/UniqueValueRenderer";
import CIMSymbol from "@arcgis/core/symbols/CIMSymbol";
import LabelClass from "@arcgis/core/layers/support/LabelClass";
import TextSymbol from "@arcgis/core/symbols/TextSymbol";
import Font from "@arcgis/core/symbols/Font";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

/**
 * Build a CIM symbol that renders as a colored circle with the score number
 * displayed inside it. CIM (Cartographic Information Model) lets us create
 * rich multi-layer symbols beyond what SimpleMarkerSymbol supports.
 */
type CIMColor = [number, number, number, number];

function makeScoreBadge(fillColor: number[], size: number): CIMSymbol {
  const [r, g, b] = fillColor;
  return new CIMSymbol({
    data: {
      type: "CIMSymbolReference",
      symbol: {
        type: "CIMPointSymbol",
        symbolLayers: [
          // Outer glow / halo for visibility against the basemap
          {
            type: "CIMVectorMarker",
            enable: true,
            size: size + 8,
            frame: { xmin: 0, ymin: 0, xmax: 20, ymax: 20 },
            markerGraphics: [
              {
                type: "CIMMarkerGraphic",
                geometry: {
                  rings: [
                    Array.from({ length: 64 }, (_, i) => {
                      const angle = (i / 64) * 2 * Math.PI;
                      return [10 + 10 * Math.cos(angle), 10 + 10 * Math.sin(angle)];
                    }),
                  ],
                },
                symbol: {
                  type: "CIMPolygonSymbol",
                  symbolLayers: [
                    {
                      type: "CIMSolidFill",
                      enable: true,
                      color: [r, g, b, 60] as CIMColor, // semi-transparent glow
                    },
                  ],
                },
              },
            ],
          },
          // Main circle
          {
            type: "CIMVectorMarker",
            enable: true,
            size,
            frame: { xmin: 0, ymin: 0, xmax: 20, ymax: 20 },
            markerGraphics: [
              {
                type: "CIMMarkerGraphic",
                geometry: {
                  rings: [
                    Array.from({ length: 64 }, (_, i) => {
                      const angle = (i / 64) * 2 * Math.PI;
                      return [10 + 10 * Math.cos(angle), 10 + 10 * Math.sin(angle)];
                    }),
                  ],
                },
                symbol: {
                  type: "CIMPolygonSymbol",
                  symbolLayers: [
                    {
                      type: "CIMSolidStroke",
                      enable: true,
                      color: [255, 255, 255, 200],
                      width: 1.5,
                    },
                    {
                      type: "CIMSolidFill",
                      enable: true,
                      color: [r, g, b, 230] as CIMColor,
                    },
                  ],
                },
              },
            ],
          },
        ],
      },
    },
  });
}

// Score tiers with colors and sizes
const scoreTiers = [
  { min: 80, max: 100, color: [139, 92, 246], size: 30, label: "Epic (80-100)" },       // violet
  { min: 60, max: 79,  color: [99, 102, 241], size: 28, label: "Great (60-79)" },        // indigo
  { min: 40, max: 59,  color: [56, 189, 248], size: 26, label: "Good (40-59)" },         // sky
  { min: 20, max: 39,  color: [125, 211, 252], size: 24, label: "Fair (20-39)" },        // sky-300
  { min: 0,  max: 19,  color: [148, 163, 184], size: 22, label: "Poor (0-19)" },         // slate
];

/**
 * We use an Arcade expression to bucket each resort's condition_score
 * into a tier string, then UniqueValueRenderer maps each tier to a CIM symbol.
 */
const arcadeExpression = `
  var score = $feature.condition_score;
  When(
    score >= 80, 'epic',
    score >= 60, 'great',
    score >= 40, 'good',
    score >= 20, 'fair',
    score >= 0,  'poor',
    'none'
  )
`;

const renderer = new UniqueValueRenderer({
  valueExpression: arcadeExpression,
  defaultSymbol: makeScoreBadge([71, 85, 105], 20), // slate-600 for null scores
  uniqueValueInfos: [
    { value: "epic",  symbol: makeScoreBadge(scoreTiers[0].color, scoreTiers[0].size), label: scoreTiers[0].label },
    { value: "great", symbol: makeScoreBadge(scoreTiers[1].color, scoreTiers[1].size), label: scoreTiers[1].label },
    { value: "good",  symbol: makeScoreBadge(scoreTiers[2].color, scoreTiers[2].size), label: scoreTiers[2].label },
    { value: "fair",  symbol: makeScoreBadge(scoreTiers[3].color, scoreTiers[3].size), label: scoreTiers[3].label },
    { value: "poor",  symbol: makeScoreBadge(scoreTiers[4].color, scoreTiers[4].size), label: scoreTiers[4].label },
  ],
});

/**
 * Label class — displays the condition score number on top of each marker.
 */
const scoreLabel = new LabelClass({
  labelExpressionInfo: {
    expression: `
      var score = $feature.condition_score;
      IIF(IsEmpty(score), '--', Text(score, '#'))
    `,
  },
  symbol: new TextSymbol({
    color: "white",
    haloColor: "rgba(0,0,0,0.85)",
    haloSize: 2.5,
    font: new Font({ size: 11, weight: "bold", family: "Inter, sans-serif" }),
    yoffset: 0,
  }),
  minScale: 0,
  maxScale: 0,
});

/**
 * Label class for resort names — only visible when zoomed in enough
 * so names don't overlap at continent scale.
 */
const nameLabel = new LabelClass({
  labelExpressionInfo: {
    expression: `$feature.name`,
  },
  symbol: new TextSymbol({
    color: "white",
    haloColor: "rgba(15, 23, 42, 0.8)",
    haloSize: 1.5,
    font: new Font({ size: 11, weight: "bold", family: "Inter, sans-serif" }),
    yoffset: -20,
  }),
  minScale: 3000000,
  maxScale: 0,
});

const popupTemplate = {
  title: "{name}",
  content: `
    <div style="font-family: Inter, sans-serif; line-height: 1.6;">
      <div style="font-size: 28px; font-weight: bold; color: #60a5fa; margin-bottom: 4px;">
        {condition_score}<span style="font-size: 14px; color: #94a3b8;">/100</span>
      </div>
      <div style="color: #94a3b8; margin-bottom: 12px;">{score_explanation}</div>
      <table style="width: 100%; font-size: 13px;">
        <tr><td style="color: #94a3b8;">Snow Depth</td><td style="text-align:right; font-weight:600;">{current_snow_depth_in}"</td></tr>
        <tr><td style="color: #94a3b8;">48h Snowfall</td><td style="text-align:right; font-weight:600;">{snowfall_48h_in}"</td></tr>
        <tr><td style="color: #94a3b8;">7d Snowfall</td><td style="text-align:right; font-weight:600;">{snowfall_7d_in}"</td></tr>
        <tr><td style="color: #94a3b8;">Trend</td><td style="text-align:right; font-weight:600;">{snowpack_trend}</td></tr>
        <tr><td style="color: #94a3b8;">Elevation</td><td style="text-align:right; font-weight:600;">{base_elevation_ft}-{summit_elevation_ft} ft</td></tr>
      </table>
    </div>
  `,
};

interface MapComponentProps {
  onResortSelect?: (id: number) => void;
  stateFilter?: string | null;
}

export default function MapComponent({ onResortSelect, stateFilter }: MapComponentProps) {
  const mapDiv = useRef<HTMLDivElement>(null);
  const viewRef = useRef<MapViewEsri | null>(null);
  const layerRef = useRef<GeoJSONLayer | null>(null);
  const [selectedResort, setSelectedResort] = useState<ResortDetail | null>(null);
  const [panelOpen, setPanelOpen] = useState(false);

  useEffect(() => {
    if (!mapDiv.current) return;

    const apiKey = process.env.NEXT_PUBLIC_ARCGIS_API_KEY || "";
    esriConfig.apiKey = apiKey;

    const geojsonLayer = new GeoJSONLayer({
      url: `${API_URL}/export/geojson`,
      title: "Resort Conditions",
      renderer,
      popupTemplate,
      labelingInfo: [scoreLabel, nameLabel],
      labelsVisible: true,
      outFields: ["*"],
    });

    layerRef.current = geojsonLayer;

    // In @arcgis/core v5, "arcgis/*" IDs belong to the v2 Basemap Styles service
    // and must be passed via `style`, not as a plain basemap id string.
    // Fall back to OpenStreetMap (no key required) if no API key is configured.
    const basemap = apiKey
      ? new Basemap({ style: { id: "arcgis/topographic" } })
      : Basemap.fromId("osm");

    const map = new Map({
      basemap,
      layers: [geojsonLayer],
    });

    const view = new MapViewEsri({
      container: mapDiv.current,
      map,
      center: [-119.5, 40.0],
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
            view.popup?.close();
          } catch (err) {
            console.error("Error fetching resort detail:", err);
          }
        }
      }
    });

    return () => {
      view.destroy();
    };
  }, []);

  useEffect(() => {
    if (!layerRef.current) return;
    layerRef.current.definitionExpression = stateFilter
      ? `state = '${stateFilter}'`
      : "";
  }, [stateFilter]);

  useEffect(() => {
    if (!viewRef.current) return;
    if (!stateFilter) {
      viewRef.current.goTo({ center: [-119.5, 40.0], zoom: 5 }, { duration: 800 });
      return;
    }
    const targets: Record<string, { center: [number, number]; zoom: number }> = {
      CA: { center: [-119.5, 38.5], zoom: 7 },
      CO: { center: [-106.2, 39.5], zoom: 7 },
      WA: { center: [-121.2, 47.5], zoom: 7 },
    };
    const t = targets[stateFilter];
    if (t) viewRef.current.goTo(t, { duration: 800 });
  }, [stateFilter]);

  return (
    <div className="relative w-full h-full">
      <div ref={mapDiv} className="w-full h-full" />

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
