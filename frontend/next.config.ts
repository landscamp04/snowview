import type { NextConfig } from "next";

const nextConfig = {
  async rewrites() {
    return [
      {
        source: "/backend/:path*",
        destination: "http://54.151.34.78:8000/api/:path*",
      },
    ];
  },
};

module.exports = nextConfig;
