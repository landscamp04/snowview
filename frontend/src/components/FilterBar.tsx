"use client";

interface FilterBarProps {
  stateFilter: string | null;
  onStateChange: (state: string | null) => void;
}

const states = [
  { value: null, label: "All States" },
  { value: "CA", label: "California" },
  { value: "CO", label: "Colorado" },
  { value: "WA", label: "Washington" },
];

export default function FilterBar({ stateFilter, onStateChange }: FilterBarProps) {
  return (
    <div className="absolute top-4 left-4 z-10 bg-slate-900/90 backdrop-blur rounded-lg border border-slate-700/50 p-2 flex gap-1">
      {states.map((s) => (
        <button
          key={s.label}
          onClick={() => onStateChange(s.value)}
          className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
            stateFilter === s.value
              ? "bg-blue-600 text-white"
              : "text-slate-300 hover:bg-slate-800 hover:text-white"
          }`}
        >
          {s.label}
        </button>
      ))}
    </div>
  );
}