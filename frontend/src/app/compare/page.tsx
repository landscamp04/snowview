"use client";

import { useEffect, useState } from "react";
import { getResorts, compareResorts } from "@/lib/api";
import type { Resort, CompareResult } from "@/types";

// Same score color logic as the map/panel for consistency
function scoreColor(score: number | null): string {
  if (score === null) return "bg-slate-600";
  if (score >= 80) return "bg-violet-500";
  if (score >= 60) return "bg-indigo-500";
  if (score >= 40) return "bg-sky-500";
  if (score >= 20) return "bg-sky-300 text-slate-900";
  return "bg-slate-500";
}

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

export default function ComparePage() {
  const [allResorts, setAllResorts] = useState<Resort[]>([]);
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [result, setResult] = useState<CompareResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load all resorts on mount for the selector
  useEffect(() => {
    getResorts().then(setAllResorts).catch(console.error);
  }, []);

  // Toggle resort selection (max 3)
  function toggleResort(id: number) {
    setSelectedIds((prev) => {
      if (prev.includes(id)) return prev.filter((x) => x !== id);
      if (prev.length >= 3) return prev; // cap at 3
      return [...prev, id];
    });
    setResult(null); // clear previous comparison
  }

  // Run comparison
  async function handleCompare() {
    if (selectedIds.length < 2) return;
    setLoading(true);
    setError(null);
    try {
      const data = await compareResorts(selectedIds);
      setResult(data);
    } catch (err) {
      setError("Failed to load comparison. Make sure the backend is running.");
      console.error(err);
    } finally {
      setLoading(false);
    }
  }

  // Group resorts by state for the selector
  const byState = allResorts.reduce(
    (acc, r) => {
      if (!acc[r.state]) acc[r.state] = [];
      acc[r.state].push(r);
      return acc;
    },
    {} as Record<string, Resort[]>
  );

  return (
    <div className="max-w-5xl mx-auto px-4 py-10">
      <h1 className="text-3xl font-bold mb-2">Compare Resorts</h1>
      <p className="text-slate-400 mb-8">
        Select 2-3 resorts to see conditions side by side.
      </p>

      {/* Resort Selector */}
      <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-5 mb-8">
        {Object.entries(byState)
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([state, resorts]) => (
            <div key={state} className="mb-4 last:mb-0">
              <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
                {state === "CA" ? "California" : state === "CO" ? "Colorado" : "Washington"}
              </p>
              <div className="flex flex-wrap gap-2">
                {resorts.map((r) => {
                  const selected = selectedIds.includes(r.id);
                  return (
                    <button
                      key={r.id}
                      onClick={() => toggleResort(r.id)}
                      className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                        selected
                          ? "bg-blue-600 text-white ring-2 ring-blue-400/50"
                          : "bg-slate-800 text-slate-300 hover:bg-slate-700"
                      } ${
                        !selected && selectedIds.length >= 3
                          ? "opacity-40 cursor-not-allowed"
                          : ""
                      }`}
                      disabled={!selected && selectedIds.length >= 3}
                    >
                      {r.name}
                      {r.condition_score !== null && (
                        <span className="ml-1.5 text-xs opacity-70">{r.condition_score}</span>
                      )}
                    </button>
                  );
                })}
              </div>
            </div>
          ))}

        <div className="mt-5 flex items-center gap-4">
          <button
            onClick={handleCompare}
            disabled={selectedIds.length < 2 || loading}
            className="bg-blue-600 hover:bg-blue-500 disabled:bg-slate-700 disabled:text-slate-500 text-white font-semibold px-6 py-2 rounded-lg transition-colors"
          >
            {loading ? "Comparing..." : `Compare (${selectedIds.length}/3)`}
          </button>
          {selectedIds.length > 0 && (
            <button
              onClick={() => {
                setSelectedIds([]);
                setResult(null);
              }}
              className="text-sm text-slate-400 hover:text-white transition-colors"
            >
              Clear selection
            </button>
          )}
        </div>
      </div>

      {error && (
        <p className="text-red-400 mb-6">{error}</p>
      )}

      {/* Comparison Results */}
      {result && (
        <div className="space-y-8">
          {/* Winner Banner */}
          {result.winners.overall && (
            <div className="bg-gradient-to-r from-violet-900/30 to-indigo-900/30 border border-violet-500/30 rounded-xl p-5 text-center">
              <p className="text-sm text-violet-300 mb-1">Best Conditions Overall</p>
              <p className="text-2xl font-bold text-white">{result.winners.overall}</p>
            </div>
          )}

          {/* Side-by-side Cards */}
          <div className={`grid gap-4 ${
            result.resorts.length === 2 ? "md:grid-cols-2" : "md:grid-cols-3"
          }`}>
            {result.resorts.map((r) => (
              <div
                key={r.id}
                className={`bg-slate-800/50 border rounded-xl p-5 ${
                  r.name === result.winners.overall
                    ? "border-violet-500/50 ring-1 ring-violet-500/20"
                    : "border-slate-700/50"
                }`}
              >
                {/* Header */}
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h3 className="font-bold text-white text-lg">{r.name}</h3>
                    <p className="text-sm text-slate-400">{r.state}</p>
                  </div>
                  <div
                    className={`${scoreColor(r.condition_score)} text-white text-xl font-bold rounded-lg w-14 h-14 flex items-center justify-center`}
                  >
                    {r.condition_score ?? "—"}
                  </div>
                </div>

                {/* Metrics */}
                <div className="space-y-3">
                  <MetricRow
                    label="Snow Depth"
                    value={r.current_snow_depth_in}
                    unit='"'
                    isWinner={r.name === result.winners.deepest_base}
                  />
                  <MetricRow
                    label="48h Snowfall"
                    value={r.snowfall_48h_in}
                    unit='"'
                    isWinner={r.name === result.winners.most_snow_48h}
                  />
                  <MetricRow
                    label="7d Snowfall"
                    value={r.snowfall_7d_in}
                    unit='"'
                  />
                  <MetricRow
                    label="SWE"
                    value={r.swe_in}
                    unit='"'
                  />
                  <MetricRow
                    label="72h Forecast"
                    value={r.forecast_snowfall_72h_in}
                    unit='"'
                    isWinner={r.name === result.winners.best_forecast}
                  />
                  <MetricRow
                    label="Avg Temp"
                    value={r.temp_avg_f}
                    unit="°F"
                  />

                  {/* Trend */}
                  <div className="flex items-center justify-between pt-1 border-t border-slate-700/50">
                    <span className="text-sm text-slate-400">Snowpack Trend</span>
                    <span className={`text-sm font-semibold ${trendColor(r.snowpack_trend)}`}>
                      {trendIcon(r.snowpack_trend)} {r.snowpack_trend ?? "—"}
                    </span>
                  </div>
                </div>

                {/* Explanation */}
                {r.score_explanation && (
                  <p className="text-xs text-slate-500 mt-4">{r.score_explanation}</p>
                )}
              </div>
            ))}
          </div>

          {/* Category Winners */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <WinnerBadge label="Overall" winner={result.winners.overall} />
            <WinnerBadge label="Most Snow (48h)" winner={result.winners.most_snow_48h} />
            <WinnerBadge label="Deepest Base" winner={result.winners.deepest_base} />
            <WinnerBadge label="Best Forecast" winner={result.winners.best_forecast} />
          </div>
        </div>
      )}
    </div>
  );
}

function MetricRow({
  label,
  value,
  unit,
  isWinner = false,
}: {
  label: string;
  value: number | null;
  unit: string;
  isWinner?: boolean;
}) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-sm text-slate-400">{label}</span>
      <span className={`text-sm font-semibold ${isWinner ? "text-violet-300" : "text-white"}`}>
        {value !== null ? `${value}${unit}` : "—"}
        {isWinner && <span className="ml-1 text-xs">★</span>}
      </span>
    </div>
  );
}

function WinnerBadge({ label, winner }: { label: string; winner: string }) {
  return (
    <div className="bg-slate-800/50 border border-slate-700/50 rounded-lg p-3 text-center">
      <p className="text-xs text-slate-500 mb-1">{label}</p>
      <p className="text-sm font-semibold text-white truncate">{winner}</p>
    </div>
  );
}