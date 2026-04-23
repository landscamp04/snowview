"use client";

import type { ResortDetail } from "@/types";
import Link from "next/link";

interface ResortPanelProps {
  resort: ResortDetail;
  onClose: () => void;
}

// Maps the 0-100 score to a color for the score badge
function scoreColor(score: number | null): string {
  if (score === null) return "bg-slate-600";
  if (score >= 80) return "bg-violet-500";
  if (score >= 60) return "bg-indigo-500";
  if (score >= 40) return "bg-sky-500";
  if (score >= 20) return "bg-sky-300 text-slate-900";
  return "bg-slate-500";
}

// Arrow icons for snowpack trend
function trendIcon(trend: string | null): string {
  if (trend === "rising") return "↑";
  if (trend === "declining") return "↓";
  return "→";
}

function trendColor(trend: string | null): string {
  if (trend === "rising") return "text-green-400";
  if (trend === "declining") return "text-red-400";
  return "text-slate-400";
}

export default function ResortPanel({ resort, onClose }: ResortPanelProps) {
  const c = resort.conditions;

  return (
    <div className="absolute top-0 right-0 h-full w-96 bg-slate-900/95 backdrop-blur border-l border-slate-700/50 overflow-y-auto shadow-2xl animate-in slide-in-from-right">
      {/* Header */}
      <div className="sticky top-0 bg-slate-900/95 backdrop-blur border-b border-slate-700/50 p-4">
        <div className="flex items-start justify-between">
          <div>
            <h2 className="text-lg font-bold text-white">{resort.name}</h2>
            <p className="text-sm text-slate-400">
              {resort.state} · {resort.base_elevation_ft?.toLocaleString()}-
              {resort.summit_elevation_ft?.toLocaleString()} ft
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white text-xl leading-none p-1"
          >
            ×
          </button>
        </div>
      </div>

      <div className="p-4 space-y-5">
        {/* Score Badge */}
        <div className="flex items-center gap-4">
          <div
            className={`${scoreColor(c.condition_score)} text-white text-3xl font-bold rounded-xl w-20 h-20 flex items-center justify-center`}
          >
            {c.condition_score ?? "—"}
          </div>
          <div>
            <p className="text-sm text-slate-400">Condition Score</p>
            {c.score_explanation && (
              <p className="text-sm text-slate-300 mt-1">{c.score_explanation}</p>
            )}
            {c.computed_date && (
              <p className="text-xs text-slate-500 mt-1">Updated {c.computed_date}</p>
            )}
          </div>
        </div>

        {/* Key Metrics Grid */}
        <div className="grid grid-cols-2 gap-3">
          <MetricCard label="Snow Depth" value={c.current_snow_depth_in} unit='"' />
          <MetricCard label="48h Snowfall" value={c.snowfall_48h_in} unit='"' />
          <MetricCard label="7d Snowfall" value={c.snowfall_7d_in} unit='"' />
          <MetricCard label="SWE" value={c.swe_in} unit='"' />
          <MetricCard label="72h Forecast" value={c.forecast_snowfall_72h_in} unit='"' />
          <MetricCard label="Avg Temp" value={c.temp_avg_f} unit="°F" />
        </div>

        {/* Snowpack Trend */}
        <div className="bg-slate-800/50 rounded-lg p-3 flex items-center justify-between">
          <span className="text-sm text-slate-400">Snowpack Trend</span>
          <span className={`font-semibold ${trendColor(c.snowpack_trend)}`}>
            {trendIcon(c.snowpack_trend)} {c.snowpack_trend ?? "Unknown"}
          </span>
        </div>

        {/* Forecast */}
        {resort.forecast.length > 0 && (
          <div>
            <h3 className="text-sm font-semibold text-slate-300 mb-2">7-Day Forecast</h3>
            <div className="space-y-1.5">
              {resort.forecast.map((f) => (
                <div
                  key={f.date}
                  className="flex items-center justify-between bg-slate-800/30 rounded px-3 py-1.5 text-sm"
                >
                  <span className="text-slate-400">
                    {new Date(f.date + "T00:00").toLocaleDateString("en-US", {
                      weekday: "short",
                      month: "short",
                      day: "numeric",
                    })}
                  </span>
                  <div className="flex items-center gap-3">
                    {f.snowfall_in !== null && f.snowfall_in > 0 && (
                      <span className="text-blue-300 font-medium">{f.snowfall_in}"</span>
                    )}
                    <span className="text-slate-300">
                      {f.temp_high_f !== null ? `${Math.round(f.temp_high_f)}°` : "—"}/
                      {f.temp_low_f !== null ? `${Math.round(f.temp_low_f)}°` : "—"}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Linked SNOTEL Stations */}
        {resort.stations.length > 0 && (
          <div>
            <h3 className="text-sm font-semibold text-slate-300 mb-2">
              SNOTEL Stations ({resort.stations.length})
            </h3>
            <div className="space-y-1.5">
              {resort.stations.map((s) => (
                <div
                  key={s.name}
                  className="flex items-center justify-between bg-slate-800/30 rounded px-3 py-1.5 text-sm"
                >
                  <span className="text-slate-300">{s.name}</span>
                  <span className="text-slate-500">
                    {s.elevation_ft?.toLocaleString()} ft · {s.distance_miles} mi
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Actions */}
        <div className="flex gap-2 pt-2">
          <Link
            href={`/history/${resort.id}`}
            className="flex-1 text-center bg-slate-800 hover:bg-slate-700 text-sm font-medium text-white px-4 py-2 rounded-lg transition-colors"
          >
            View History
          </Link>
          {resort.website_url && (
            <a
              href={resort.website_url}
              target="_blank"
              rel="noopener noreferrer"
              className="flex-1 text-center bg-slate-800 hover:bg-slate-700 text-sm font-medium text-white px-4 py-2 rounded-lg transition-colors"
            >
              Resort Website
            </a>
          )}
        </div>
      </div>
    </div>
  );
}

// Reusable metric card component
function MetricCard({
  label,
  value,
  unit,
}: {
  label: string;
  value: number | null;
  unit: string;
}) {
  return (
    <div className="bg-slate-800/50 rounded-lg p-3">
      <p className="text-xs text-slate-400">{label}</p>
      <p className="text-lg font-semibold text-white mt-0.5">
        {value !== null ? `${value}${unit}` : "—"}
      </p>
    </div>
  );
}