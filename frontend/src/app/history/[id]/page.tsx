"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { getResortHistory, getResort } from "@/lib/api";
import type { ResortHistory, ResortDetail } from "@/types";
import {
  AreaChart,
  Area,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

export default function HistoryPage() {
  const params = useParams();
  const resortId = Number(params.id);

  const [history, setHistory] = useState<ResortHistory | null>(null);
  const [resort, setResort] = useState<ResortDetail | null>(null);
  const [days, setDays] = useState(30);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!resortId) return;
    setLoading(true);

    Promise.all([
      getResortHistory(resortId, days),
      getResort(resortId),
    ])
      .then(([h, r]) => {
        setHistory(h);
        setResort(r);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [resortId, days]);

  if (loading) {
    return (
      <div className="max-w-5xl mx-auto px-4 py-10">
        <p className="text-slate-400">Loading history...</p>
      </div>
    );
  }

  if (!history || !resort) {
    return (
      <div className="max-w-5xl mx-auto px-4 py-10">
        <p className="text-red-400">Resort not found.</p>
        <Link href="/map" className="text-blue-400 mt-4 inline-block">
          ← Back to map
        </Link>
      </div>
    );
  }

  // Format observation data for recharts
  const chartData = history.observations.map((o) => ({
    date: new Date(o.date + "T00:00").toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    }),
    rawDate: o.date,
    snow_depth: o.snow_depth_in,
    swe: o.swe_in,
    temp_max: o.temp_max_f,
    temp_min: o.temp_min_f,
  }));

  return (
    <div className="max-w-5xl mx-auto px-4 py-10">
      {/* Header */}
      <div className="flex items-start justify-between mb-8">
        <div>
          <Link href="/map" className="text-sm text-blue-400 hover:text-blue-300 mb-2 inline-block">
            ← Back to map
          </Link>
          <h1 className="text-3xl font-bold">{resort.name}</h1>
          <p className="text-slate-400">
            {resort.state} · {resort.base_elevation_ft?.toLocaleString()}-
            {resort.summit_elevation_ft?.toLocaleString()} ft
          </p>
        </div>

        {/* Day range selector */}
        <div className="flex gap-1 bg-slate-800 rounded-lg p-1">
          {[7, 30, 60].map((d) => (
            <button
              key={d}
              onClick={() => setDays(d)}
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                days === d
                  ? "bg-blue-600 text-white"
                  : "text-slate-400 hover:text-white"
              }`}
            >
              {d}d
            </button>
          ))}
        </div>
      </div>

      {/* Snow Depth Chart */}
      <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-5 mb-6">
        <h2 className="text-lg font-semibold mb-4">Snow Depth & SWE</h2>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={chartData}>
            <defs>
              <linearGradient id="snowGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#38bdf8" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="sweGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#818cf8" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#818cf8" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis
              dataKey="date"
              stroke="#64748b"
              tick={{ fontSize: 11 }}
              interval="preserveStartEnd"
            />
            <YAxis stroke="#64748b" tick={{ fontSize: 11 }} />
            <Tooltip
              contentStyle={{
                backgroundColor: "#1e293b",
                border: "1px solid #334155",
                borderRadius: "8px",
                fontSize: "13px",
              }}
              labelStyle={{ color: "#94a3b8" }}
            />
            <Legend />
            <Area
              type="monotone"
              dataKey="snow_depth"
              name="Snow Depth (in)"
              stroke="#38bdf8"
              fill="url(#snowGradient)"
              strokeWidth={2}
            />
            <Area
              type="monotone"
              dataKey="swe"
              name="SWE (in)"
              stroke="#818cf8"
              fill="url(#sweGradient)"
              strokeWidth={2}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Temperature Chart */}
      <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-5 mb-6">
        <h2 className="text-lg font-semibold mb-4">Temperature Range</h2>
        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis
              dataKey="date"
              stroke="#64748b"
              tick={{ fontSize: 11 }}
              interval="preserveStartEnd"
            />
            <YAxis stroke="#64748b" tick={{ fontSize: 11 }} unit="°F" />
            <Tooltip
              contentStyle={{
                backgroundColor: "#1e293b",
                border: "1px solid #334155",
                borderRadius: "8px",
                fontSize: "13px",
              }}
              labelStyle={{ color: "#94a3b8" }}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="temp_max"
              name="High (°F)"
              stroke="#f87171"
              strokeWidth={2}
              dot={false}
            />
            <Line
              type="monotone"
              dataKey="temp_min"
              name="Low (°F)"
              stroke="#60a5fa"
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Current Conditions Summary */}
      <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-5">
        <h2 className="text-lg font-semibold mb-4">Current Conditions</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Condition Score"
            value={resort.conditions.condition_score}
            unit="/100"
          />
          <StatCard
            label="Snow Depth"
            value={resort.conditions.current_snow_depth_in}
            unit='"'
          />
          <StatCard
            label="48h Snowfall"
            value={resort.conditions.snowfall_48h_in}
            unit='"'
          />
          <StatCard
            label="72h Forecast"
            value={resort.conditions.forecast_snowfall_72h_in}
            unit='"'
          />
        </div>
      </div>
    </div>
  );
}

function StatCard({
  label,
  value,
  unit,
}: {
  label: string;
  value: number | null;
  unit: string;
}) {
  return (
    <div className="text-center">
      <p className="text-2xl font-bold text-white">
        {value !== null ? `${value}${unit}` : "—"}
      </p>
      <p className="text-xs text-slate-400 mt-1">{label}</p>
    </div>
  );
}