"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { getTopResorts, getStatus } from "@/lib/api";
import type { Resort, SystemStatus } from "@/types";

export default function Home() {
  const [topResorts, setTopResorts] = useState<Resort[]>([]);
  const [status, setStatus] = useState<SystemStatus | null>(null);

  useEffect(() => {
    getTopResorts(undefined, 3).then(setTopResorts).catch(console.error);
    getStatus().then(setStatus).catch(console.error);
  }, []);

  return (
    <div className="min-h-screen">
      {/* Hero Section — personal photo as the container background */}
      <section
        className="relative flex flex-col items-center justify-center text-center px-4 py-32"
        style={{
          backgroundImage: "url('/images/lp-background.png')",
          backgroundSize: "cover",
          backgroundPosition: "center 40%",
        }}
      >
        {/* Soft dark overlay to keep text legible without washing out the photo */}
        <div className="absolute inset-0 bg-gradient-to-b from-black/30 via-black/35 to-slate-950" />

        <div className="relative z-10 max-w-3xl">
          <h1 className="text-5xl md:text-6xl font-bold mb-4 text-white drop-shadow-md">
            SnowView
          </h1>
          <p className="text-xl md:text-2xl text-white/90 mb-2">
            Real-time snow intelligence across the West
          </p>
          <p className="text-white/70 mb-10 max-w-xl mx-auto">
            Aggregating SNOTEL snowpack data and NOAA forecasts for ski resorts
            across California, Colorado, and Washington.
          </p>

          <Link
            href="/map"
            className="inline-block bg-blue-600 hover:bg-blue-500 text-white font-semibold px-8 py-3 rounded-lg transition-colors text-lg shadow-lg"
          >
            Explore the Map
          </Link>
        </div>
      </section>

      {/* Stats Bar */}
      {status && (
        <section className="border-y border-slate-800 bg-slate-900/50 py-8">
          <div className="max-w-4xl mx-auto grid grid-cols-2 md:grid-cols-4 gap-6 text-center px-4">
            <div>
              <p className="text-3xl font-bold text-blue-400">{status.data.resorts}</p>
              <p className="text-sm text-slate-400 mt-1">Resorts Tracked</p>
            </div>
            <div>
              <p className="text-3xl font-bold text-blue-400">{status.data.stations}</p>
              <p className="text-sm text-slate-400 mt-1">SNOTEL Stations</p>
            </div>
            <div>
              <p className="text-3xl font-bold text-blue-400">
                {status.data.observations.toLocaleString()}
              </p>
              <p className="text-sm text-slate-400 mt-1">Observations</p>
            </div>
            <div>
              <p className="text-3xl font-bold text-blue-400">3</p>
              <p className="text-sm text-slate-400 mt-1">States Covered</p>
            </div>
          </div>
        </section>
      )}

      {/* Top Conditions */}
      {topResorts.length > 0 && (
        <section className="max-w-4xl mx-auto px-4 py-16">
          <h2 className="text-2xl font-bold mb-6 text-center">Best Conditions Right Now</h2>
          <div className="grid md:grid-cols-3 gap-4">
            {topResorts.map((resort) => (
              <div
                key={resort.id}
                className="bg-slate-800/50 border border-slate-700/50 rounded-xl p-5 hover:border-blue-500/50 transition-colors"
              >
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <p className="font-semibold text-white">{resort.name}</p>
                    <p className="text-sm text-slate-400">{resort.state}</p>
                  </div>
                  <div className="text-right">
                    <p className="text-2xl font-bold text-blue-400">
                      {resort.condition_score ?? "—"}
                    </p>
                    <p className="text-xs text-slate-500">/ 100</p>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div>
                    <p className="text-slate-400">Snow Depth</p>
                    <p className="text-white font-medium">
                      {resort.current_snow_depth_in ?? "—"}&quot;
                    </p>
                  </div>
                  <div>
                    <p className="text-slate-400">48h Snowfall</p>
                    <p className="text-white font-medium">
                      {resort.snowfall_48h_in ?? "—"}&quot;
                    </p>
                  </div>
                </div>
                {resort.score_explanation && (
                  <p className="text-xs text-slate-500 mt-3">{resort.score_explanation}</p>
                )}
              </div>
            ))}
          </div>
          <div className="text-center mt-8">
            <Link
              href="/map"
              className="text-blue-400 hover:text-blue-300 font-medium text-sm transition-colors"
            >
              View all resorts on the map →
            </Link>
          </div>
        </section>
      )}
    </div>
  );
}
