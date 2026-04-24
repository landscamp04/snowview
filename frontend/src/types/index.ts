// Matches the JSON shape returned by GET /api/resorts
export interface Resort {
  id: number;
  name: string;
  state: "CA" | "CO" | "WA";
  base_elevation_ft: number;
  summit_elevation_ft: number;
  num_lifts: number | null;
  website_url: string | null;
  lat: number;
  lng: number;
  condition_score: number | null;
  current_snow_depth_in: number | null;
  snowfall_48h_in: number | null;
  snowfall_7d_in: number | null;
  snowpack_trend: "rising" | "stable" | "declining" | null;
  score_explanation: string | null;
  computed_date: string | null;
  swe_in: number | null;
  forecast_snowfall_72h_in: number | null;
  temp_avg_f: number | null;
}

// Matches GET /api/resorts/:id — includes full conditions, stations, forecast
export interface ResortDetail extends Resort {
  conditions: {
    condition_score: number | null;
    current_snow_depth_in: number | null;
    snowfall_48h_in: number | null;
    snowfall_7d_in: number | null;
    swe_in: number | null;
    snowpack_trend: "rising" | "stable" | "declining" | null;
    forecast_snowfall_72h_in: number | null;
    temp_avg_f: number | null;
    score_explanation: string | null;
    computed_date: string | null;
  };
  stations: {
    name: string;
    elevation_ft: number;
    distance_miles: number;
    weight: number;
  }[];
  forecast: {
    date: string;
    snowfall_in: number | null;
    temp_high_f: number | null;
    temp_low_f: number | null;
    wind_speed_mph: number | null;
  }[];
}

// Matches GET /api/conditions/history/:id
export interface ResortHistory {
  resort_name: string;
  resort_id: number;
  days: number;
  observations: {
    date: string;
    snow_depth_in: number | null;
    swe_in: number | null;
    temp_max_f: number | null;
    temp_min_f: number | null;
  }[];
  condition_scores: {
    date: string;
    score: number;
  }[];
}

// Matches GET /api/conditions/compare
export interface CompareResult {
  resorts: Resort[];
  winners: {
    overall: string;
    most_snow_48h: string;
    deepest_base: string;
    best_forecast: string;
  };
}

// Matches GET /api/status
export interface SystemStatus {
  status: string;
  data: {
    resorts: number;
    stations: number;
    observations: number;
    conditions: number;
  };
  freshness: {
    latest_observation: string | null;
    latest_conditions: string | null;
  };
  score_distribution: {
    excellent_70_plus: number;
    good_40_to_70: number;
    fair_below_40: number;
  };
}