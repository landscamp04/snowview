import type {
    Resort,
    ResortDetail,
    ResortHistory,
    CompareResult,
    SystemStatus,
  } from "@/types";
  
  const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";
  
  // Generic fetch wrapper with error handling
  async function fetchAPI<T>(endpoint: string): Promise<T> {
    const res = await fetch(`${API}${endpoint}`);
    if (!res.ok) {
      throw new Error(`API error: ${res.status} ${res.statusText}`);
    }
    return res.json();
  }
  
  // --- Resort endpoints ---
  
  export async function getResorts(params?: {
    state?: string;
    min_score?: number;
  }): Promise<Resort[]> {
    const query = new URLSearchParams();
    if (params?.state) query.set("state", params.state);
    if (params?.min_score) query.set("min_score", String(params.min_score));
    const qs = query.toString();
    return fetchAPI<Resort[]>(`/resorts${qs ? `?${qs}` : ""}`);
  }
  
  export async function getResort(id: number): Promise<ResortDetail> {
    return fetchAPI<ResortDetail>(`/resorts/${id}`);
  }
  
  export async function getTopResorts(
    state?: string,
    limit = 5
  ): Promise<Resort[]> {
    const query = new URLSearchParams({ limit: String(limit) });
    if (state) query.set("state", state);
    return fetchAPI<Resort[]>(`/resorts/top?${query}`);
  }
  
  export async function getNearbyResorts(
    lat: number,
    lng: number,
    radiusMiles = 100
  ): Promise<Resort[]> {
    const query = new URLSearchParams({
      lat: String(lat),
      lng: String(lng),
      radius_miles: String(radiusMiles),
    });
    return fetchAPI<Resort[]>(`/resorts/nearby?${query}`);
  }
  
  // --- Conditions endpoints ---
  
  export async function compareResorts(ids: number[]): Promise<CompareResult> {
    return fetchAPI<CompareResult>(
      `/conditions/compare?ids=${ids.join(",")}`
    );
  }
  
  export async function getResortHistory(
    id: number,
    days = 30
  ): Promise<ResortHistory> {
    return fetchAPI<ResortHistory>(`/conditions/history/${id}?days=${days}`);
  }
  
  // --- System ---
  
  export async function getStatus(): Promise<SystemStatus> {
    return fetchAPI<SystemStatus>("/status");
  }
  
  // --- Export ---
  
  export function getGeoJSONUrl(): string {
    return `${API}/export/geojson`;
  }